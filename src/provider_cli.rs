//! `noetl provider <verb>` — the Terraform-style CLI over the `kind: provider`
//! tool (noetl/ai-meta#189, Round 5).
//!
//! plan / drift / orphans / adopt, driven in **local mode** — no NoETL server
//! is required to *run* the tool (the live cloud GET goes straight to the
//! provider API, or to a mock via `--endpoint`).  A server is consulted only to
//! read last-known-desired (the EHDB raw eventlog tier), and even that has an
//! offline `--facts-file` path.
//!
//! ## This module is glue — it reimplements no provider logic
//!
//! - **plan / drift / adopt behavior** is the noetl-tools [`ProviderTool`],
//!   invoked through its public [`Tool::execute`].  `plan` = a dry-run;
//!   `drift` = `reconcile: report`; `adopt` = `reconcile: adopt` (dry-run →
//!   digest → confirm).  The gates (explicit `auth:` for real calls, dry-run
//!   default, digest-bound confirm, stale-digest refusal) live in the tool and
//!   are exercised as-is.
//! - **last-known-desired** comes from the noetl-tools
//!   [`provider_state`](noetl_tools::tools::provider_state) fold over raw EHDB
//!   eventlog-tier records (`extract_facts_from_tier_records` → `fold_facts`).
//! - **orphan / conflict** detection are `provider_state::detect_orphans_scoped`
//!   / `detect_stack_conflicts`.
//! - the per-resource tool config is assembled by
//!   [`noetl_executor::tools_bridge::to_tools_config`] from the playbook's own
//!   `Tool::Provider` step — the same builder the worker dispatch uses.

use anyhow::{bail, Context, Result};
use noetl_executor::playbook::{Playbook, Tool};
use noetl_executor::tools_bridge::to_tools_config;
use noetl_tools::context::ExecutionContext as ToolsExecutionContext;
use noetl_tools::registry::{AuthConfig, AuthType, Tool as ToolsRegistryTool, ToolConfig};
use noetl_tools::tools::{provider_state, ProviderTool};
use reqwest::Client;
use std::path::{Path, PathBuf};

/// `noetl provider <verb>` — infra plan / drift / orphans / adopt.
#[derive(clap::Subcommand)]
pub enum ProviderCommand {
    /// Plan: the request each declared provider resource WOULD issue.
    ///
    /// Pure dry-run — no network, no `auth:` needed (the safe default).  The
    /// Terraform-`plan` shape: read the playbook, print each resource's
    /// `would_call`.
    Plan {
        #[command(flatten)]
        common: ProviderCommonArgs,
    },
    /// Drift: last-known-desired (EHDB fold) vs live actual (GET), field by
    /// field, for every declared resource.  Needs `--auth-token` to read the
    /// live actual (drift never mutates regardless).
    Drift {
        #[command(flatten)]
        common: ProviderCommonArgs,
        /// Bearer token for the live GET (point `--endpoint` at a mock to avoid
        /// real cloud).
        #[arg(long)]
        auth_token: Option<String>,
    },
    /// Orphans: resources we own per the EHDB fold that the current playbook no
    /// longer declares (scoped to `--stack`).  Also reports cross-stack
    /// ownership conflicts.
    Orphans {
        #[command(flatten)]
        common: ProviderCommonArgs,
    },
    /// Adopt: confirm-gated take-ownership of the live actual as the new desired
    /// for one resource.  dry-run emits the diff + `plan_digest`; `--apply
    /// --confirm <digest>` accepts it.  Never mutates cloud state.
    Adopt {
        #[command(flatten)]
        common: ProviderCommonArgs,
        /// Bearer token for the live GET (adopt resolves the diff against live
        /// state even to plan).
        #[arg(long)]
        auth_token: Option<String>,
        /// Which provider step to adopt (required when the playbook declares
        /// more than one).
        #[arg(long)]
        step: Option<String>,
        /// Apply the adoption (default is dry-run — emit the diff + digest).
        #[arg(long)]
        apply: bool,
        /// The `plan_digest` echoed from a reviewed dry-run (required with
        /// `--apply`).
        #[arg(long)]
        confirm: Option<String>,
    },
}

/// Flags shared by every provider verb.
#[derive(clap::Args)]
pub struct ProviderCommonArgs {
    /// Playbook declaring the provider resources (its `kind: provider` steps).
    #[arg(long)]
    pub playbook: PathBuf,
    /// Ownership stack to scope drift / orphan detection to.  Overrides each
    /// step's own `stack:`; defaults to `<unscoped>` when neither is set.
    #[arg(long)]
    pub stack: Option<String>,
    /// NoETL server base URL to read last-known-desired from
    /// (`GET /api/ehdb/tiers/eventlog`).  Omit for a pure-local run with no
    /// prior state, or use `--facts-file` for an offline state source.
    #[arg(long)]
    pub server: Option<String>,
    /// Offline last-known-desired source: a file of raw eventlog-tier records
    /// (or event bodies, or bare provider_facts).  Accepts either a JSON array
    /// or newline-delimited JSON (JSONL — the append format `--facts-out`
    /// writes).  Mutually exclusive with `--server`.
    #[arg(long)]
    pub facts_file: Option<PathBuf>,
    /// Append the emitted `provider_fact` to this JSONL file after a successful
    /// apply (adopt / converge) — the EHDB-less git-backed state sink.  One
    /// applied fact per line; `planned` / dry-run outcomes are not written.
    /// Read back with `--facts-file <same path>`.  The billing-account id and
    /// IAM member identifiers are masked so a committed file stays clean.
    #[arg(long)]
    pub facts_out: Option<PathBuf>,
    /// Restrict the eventlog-tier fetch to one execution id.
    #[arg(long)]
    pub execution: Option<String>,
    /// GCP API endpoint override applied to every resource — **testing /
    /// emulators only** (points real GETs at a mock so a run is offline).
    #[arg(long)]
    pub endpoint: Option<String>,
    /// JSON object seeding template variables (the playbook `workload`) so
    /// templated step fields resolve.
    #[arg(long)]
    pub workload: Option<String>,
    /// Emit machine-readable JSON instead of a table.
    #[arg(long)]
    pub json: bool,
}

/// One declared provider resource, lifted out of the playbook.
struct DeclaredResource {
    step: String,
    tool: Tool,
}

/// Dispatch `noetl provider <verb>`.
pub async fn run(client: &Client, command: ProviderCommand) -> Result<()> {
    match command {
        ProviderCommand::Plan { common } => plan(&common).await,
        ProviderCommand::Drift { common, auth_token } => drift(client, &common, auth_token).await,
        ProviderCommand::Orphans { common } => orphans(client, &common).await,
        ProviderCommand::Adopt {
            common,
            auth_token,
            step,
            apply,
            confirm,
        } => adopt(&common, auth_token, step, apply, confirm).await,
    }
}

// ---------------------------------------------------------------------------
// Verbs
// ---------------------------------------------------------------------------

/// `plan` — dry-run every declared resource, print the `would_call`.
async fn plan(common: &ProviderCommonArgs) -> Result<()> {
    let resources = load_resources(&common.playbook)?;
    let ctx = build_context(common)?;
    let tool = ProviderTool::new();

    let mut out = Vec::new();
    for r in &resources {
        let mut cfg = base_config(r, common);
        // plan is always a no-network dry-run; force it regardless of the
        // step's own dry_run and reconcile.
        set(&mut cfg, "dry_run", serde_json::json!(true));
        set(&mut cfg, "reconcile", serde_json::json!("enforce"));
        let res = tool
            .execute(&cfg, &ctx)
            .await
            .with_context(|| format!("plan: dispatching provider step {:?}", r.step))?;
        let data = res.data.unwrap_or(serde_json::Value::Null);
        out.push(serde_json::json!({
            "step": r.step,
            "action": data.get("action"),
            "converge": data.get("converge"),
            "would_call": data.get("would_call"),
            "urn": data.get("provider_fact").and_then(|f| f.get("urn")),
        }));
    }

    if common.json {
        print_json(&serde_json::json!({ "verb": "plan", "resources": out }));
    } else {
        println!("Plan — {} resource(s):\n", out.len());
        for r in &out {
            let wc = r.get("would_call").cloned().unwrap_or(serde_json::Value::Null);
            println!(
                "  • {} [{}]\n      {} {}",
                str_field(r, "step"),
                str_field(r, "action"),
                wc.get("method").and_then(|v| v.as_str()).unwrap_or("?"),
                wc.get("url").and_then(|v| v.as_str()).unwrap_or("?"),
            );
        }
    }
    Ok(())
}

/// `drift` — last-known-desired vs live actual, per resource.
async fn drift(client: &Client, common: &ProviderCommonArgs, auth_token: Option<String>) -> Result<()> {
    let resources = load_resources(&common.playbook)?;
    let ctx = build_context(common)?;
    let model = load_ownership_model(client, common).await?;
    let tool = ProviderTool::new();
    let auth = auth_token.map(bearer);

    let mut out = Vec::new();
    for r in &resources {
        if !is_mutating(&r.tool) {
            continue; // reads have no desired-vs-actual to reconcile
        }
        // 1) derive the URN via a no-network dry-run (the tool owns URN
        //    derivation — we don't reimplement it).
        let urn = resource_urn(&tool, r, common, &ctx).await?;
        // 2) look up last-known-desired for that URN from the fold.
        let known = urn
            .as_ref()
            .and_then(|u| model.owned.get(u))
            .map(|o| o.last_desired.clone());
        // 3) run the tool's `report` reconcile to compute the drift verdict.
        let mut cfg = base_config(r, common);
        set(&mut cfg, "reconcile", serde_json::json!("report"));
        if let Some(k) = &known {
            set(&mut cfg, "known_desired", k.clone());
        }
        cfg.auth = auth.clone();
        let res = tool
            .execute(&cfg, &ctx)
            .await
            .with_context(|| format!("drift: dispatching provider step {:?}", r.step))?;
        let data = res.data.unwrap_or(serde_json::Value::Null);
        out.push(serde_json::json!({
            "step": r.step,
            "urn": data.get("urn"),
            "drift": data.get("drift"),
            "diff": data.get("diff"),
            "owned": known.is_some(),
        }));
    }

    if common.json {
        print_json(&serde_json::json!({ "verb": "drift", "stack": stack_of(common), "resources": out }));
    } else {
        println!("Drift (stack: {}) — {} resource(s):\n", stack_of(common), out.len());
        for r in &out {
            let verdict = r
                .get("drift")
                .and_then(|d| d.get("drift").and_then(|v| v.as_str()))
                .or_else(|| r.get("drift").and_then(|v| v.as_str()))
                .unwrap_or("?");
            println!(
                "  • {} [{}]  {}",
                str_field(r, "step"),
                str_field(r, "urn"),
                verdict.to_uppercase()
            );
            if let Some(diff) = r.get("diff").and_then(|d| d.as_object()) {
                for (field, d) in diff {
                    println!(
                        "      {}: desired={} actual={}",
                        field,
                        d.get("desired").cloned().unwrap_or(serde_json::Value::Null),
                        d.get("actual").cloned().unwrap_or(serde_json::Value::Null),
                    );
                }
            }
        }
    }
    Ok(())
}

/// `orphans` — owned-but-undeclared resources for the stack + cross-stack
/// conflicts.
async fn orphans(client: &Client, common: &ProviderCommonArgs) -> Result<()> {
    let resources = load_resources(&common.playbook)?;
    let ctx = build_context(common)?;
    let facts = load_facts(client, common).await?;
    let model = provider_state::fold_facts(&facts);
    let stack = stack_of(common);
    let tool = ProviderTool::new();

    // Declared URN set = the URNs the current playbook's mutating resources
    // assert (derived by the tool via a no-network dry-run).
    let mut declared = Vec::new();
    for r in &resources {
        if !is_mutating(&r.tool) {
            continue;
        }
        if let Some(urn) = resource_urn(&tool, r, common, &ctx).await? {
            declared.push(urn);
        }
    }

    let orphaned = provider_state::detect_orphans_scoped(&model, &stack, &declared);
    let conflicts = provider_state::detect_stack_conflicts(&facts);

    let orphan_json: Vec<_> = orphaned
        .iter()
        .map(|o| serde_json::json!({ "urn": o.urn, "resource_type": o.resource_type, "last_desired": o.last_desired }))
        .collect();
    let conflict_json: Vec<_> = conflicts
        .iter()
        .map(|c| serde_json::json!({ "urn": c.urn, "stacks": c.stacks }))
        .collect();

    if common.json {
        print_json(&serde_json::json!({
            "verb": "orphans", "stack": stack,
            "declared": declared, "orphans": orphan_json, "conflicts": conflict_json,
        }));
    } else {
        println!("Orphans (stack: {}) — {} declared, {} orphaned:\n", stack, declared.len(), orphaned.len());
        for o in &orphaned {
            println!("  • {} ({})  — owned per EHDB, no longer declared", o.urn, o.resource_type);
        }
        if !conflicts.is_empty() {
            println!("\n⚠ cross-stack conflicts ({}):", conflicts.len());
            for c in &conflicts {
                println!("  • {} contended by stacks: {}", c.urn, c.stacks.join(", "));
            }
        }
    }
    Ok(())
}

/// `adopt` — confirm-gated take-ownership for one resource.
async fn adopt(
    common: &ProviderCommonArgs,
    auth_token: Option<String>,
    step: Option<String>,
    apply: bool,
    confirm: Option<String>,
) -> Result<()> {
    let resources = load_resources(&common.playbook)?;
    let target = pick_target(&resources, step.as_deref())?;
    let ctx = build_context(common)?;
    let model = load_ownership_model_offline_or_server(common).await?;
    let tool = ProviderTool::new();

    let urn = resource_urn(&tool, target, common, &ctx).await?;
    let known = urn
        .as_ref()
        .and_then(|u| model.owned.get(u))
        .map(|o| o.last_desired.clone());

    let mut cfg = base_config(target, common);
    set(&mut cfg, "reconcile", serde_json::json!("adopt"));
    set(&mut cfg, "dry_run", serde_json::json!(!apply));
    if let Some(k) = &known {
        set(&mut cfg, "known_desired", k.clone());
    }
    if let Some(c) = &confirm {
        set(&mut cfg, "confirm", serde_json::json!(c));
    }
    cfg.auth = auth_token.map(bearer);

    let res = tool
        .execute(&cfg, &ctx)
        .await
        .with_context(|| format!("adopt: dispatching provider step {:?}", target.step))?;
    let data = res.data.unwrap_or(serde_json::Value::Null);

    // Git-backed state sink: an applied adopt writes an ownership fact.  Dry-run
    // (outcome `planned`) is filtered out inside the helper.
    maybe_append_applied_fact(common, &data)?;

    if common.json {
        print_json(&serde_json::json!({ "verb": "adopt", "step": target.step, "result": data }));
    } else if apply {
        println!(
            "Adopt APPLY — {} [{}]: adopted={}",
            target.step,
            data.get("urn").and_then(|v| v.as_str()).unwrap_or("?"),
            data.get("adopted").and_then(|v| v.as_bool()).unwrap_or(false),
        );
    } else {
        println!(
            "Adopt DRY-RUN — {} [{}]\n  plan_digest: {}\n  review the diff, then re-run with --apply --confirm <plan_digest>",
            target.step,
            data.get("urn").and_then(|v| v.as_str()).unwrap_or("?"),
            data.get("plan_digest").and_then(|v| v.as_str()).unwrap_or("?"),
        );
        if let Some(diff) = data.get("diff").and_then(|d| d.as_object()) {
            for (field, d) in diff {
                println!(
                    "    {}: desired={} actual={}",
                    field,
                    d.get("desired").cloned().unwrap_or(serde_json::Value::Null),
                    d.get("actual").cloned().unwrap_or(serde_json::Value::Null),
                );
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Parse a playbook and lift out its `kind: provider` steps.
fn load_resources(path: &Path) -> Result<Vec<DeclaredResource>> {
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("reading playbook {}", path.display()))?;
    let pb: Playbook =
        serde_yaml::from_str(&content).with_context(|| format!("parsing playbook {}", path.display()))?;
    let mut out = Vec::new();
    for step in pb.workflow {
        if let Some(tool @ Tool::Provider { .. }) = step.tool {
            out.push(DeclaredResource { step: step.step, tool });
        }
    }
    if out.is_empty() {
        bail!(
            "playbook {} declares no `kind: provider` steps",
            path.display()
        );
    }
    Ok(out)
}

/// Parse a facts file that is EITHER a JSON array (the historical form) OR
/// newline-delimited JSON (JSONL — what `--facts-out` appends).  Detected by the
/// first non-whitespace byte: `[` → array; otherwise parse each non-empty line.
fn parse_facts_content(content: &str) -> Result<Vec<serde_json::Value>> {
    let trimmed = content.trim_start();
    if trimmed.starts_with('[') {
        return Ok(serde_json::from_str(content)?);
    }
    let mut out = Vec::new();
    for (i, line) in content.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let v: serde_json::Value = serde_json::from_str(line)
            .with_context(|| format!("parsing JSONL line {}", i + 1))?;
        out.push(v);
    }
    Ok(out)
}

/// Append one `provider_fact` to a JSONL file (create-if-absent, O_APPEND),
/// masking the non-secret-but-policy-sensitive identifiers so a committed file
/// stays clean.  Called only after a successful APPLY with an applied outcome.
fn append_fact_jsonl(path: &Path, fact: &serde_json::Value) -> Result<()> {
    use std::io::Write;
    // Wrap under `provider_fact` so the read side (`fact_in_record`, which looks
    // at `provider_fact` / `data.provider_fact` / `result.data.provider_fact`)
    // recognizes each JSONL line.  Mask sensitive identifiers first.
    let record = serde_json::json!({ "provider_fact": mask_fact_identifiers(fact) });
    let mut line = serde_json::to_string(&record)?;
    line.push('\n');
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("opening facts-out file {}", path.display()))?;
    f.write_all(line.as_bytes())
        .with_context(|| format!("appending to facts-out file {}", path.display()))?;
    Ok(())
}

/// Mask the two identifiers that can appear in a fact's `desired` and are kept
/// out of git by policy (neither is a credential): the billing account id and
/// the IAM member email.  Everything else (URN, folder/project/service ids) is
/// commit-safe.
fn mask_fact_identifiers(fact: &serde_json::Value) -> serde_json::Value {
    let mut f = fact.clone();
    if let Some(desired) = f.get_mut("desired").and_then(|d| d.as_object_mut()) {
        for k in [
            "billing_account",
            "billingAccount",
            "billingAccountName",
            "member",
        ] {
            if desired.contains_key(k) {
                desired.insert(k.to_string(), serde_json::json!("<masked>"));
            }
        }
    }
    f
}

/// Append the applied `provider_fact` from a tool result to the JSONL sink, iff
/// the outcome is an applied one (`planned` / dry-run is intent-only and is
/// skipped).  Shared by the provider verbs and `noetl exec --facts-out` so both
/// dispatch paths persist ownership identically.
pub(crate) fn append_applied_fact(out: &Path, data: &serde_json::Value) -> Result<()> {
    let Some(fact) = data.get("provider_fact") else {
        return Ok(());
    };
    let outcome = fact.get("outcome").and_then(|o| o.as_str()).unwrap_or("");
    if matches!(outcome, "changed" | "noop" | "adopted" | "deleted" | "absent") {
        append_fact_jsonl(out, fact)?;
    }
    Ok(())
}

/// `--facts-out` convenience for the provider verbs (wraps [`append_applied_fact`]).
fn maybe_append_applied_fact(common: &ProviderCommonArgs, data: &serde_json::Value) -> Result<()> {
    if let Some(out) = &common.facts_out {
        append_applied_fact(out, data)?;
    }
    Ok(())
}

/// Build the noetl-tools ToolConfig for a resource, applying the CLI's stack /
/// endpoint overrides.  Reuses the executor's dispatch-side builder.
fn base_config(r: &DeclaredResource, common: &ProviderCommonArgs) -> ToolConfig {
    let mut cfg = to_tools_config(&r.tool);
    if let Some(stack) = &common.stack {
        set(&mut cfg, "stack", serde_json::json!(stack));
    }
    if let Some(endpoint) = &common.endpoint {
        set(&mut cfg, "endpoint", serde_json::json!(endpoint));
    }
    cfg
}

/// Derive a resource's URN by running a no-network dry-run and reading the
/// `provider_fact.urn` the tool emits — so the CLI never reimplements URN
/// construction.
async fn resource_urn(
    tool: &ProviderTool,
    r: &DeclaredResource,
    common: &ProviderCommonArgs,
    ctx: &ToolsExecutionContext,
) -> Result<Option<String>> {
    let mut cfg = base_config(r, common);
    set(&mut cfg, "dry_run", serde_json::json!(true));
    set(&mut cfg, "reconcile", serde_json::json!("enforce"));
    let res = tool
        .execute(&cfg, ctx)
        .await
        .with_context(|| format!("deriving URN for step {:?}", r.step))?;
    Ok(res
        .data
        .and_then(|d| d.get("provider_fact").and_then(|f| f.get("urn")).and_then(|v| v.as_str()).map(String::from)))
}

/// Load the folded ownership model (server or offline facts file).
async fn load_ownership_model(client: &Client, common: &ProviderCommonArgs) -> Result<provider_state::OwnershipModel> {
    Ok(provider_state::fold_facts(&load_facts(client, common).await?))
}

/// Adopt path: no shared `client` in scope, build one locally when a server is
/// configured.
async fn load_ownership_model_offline_or_server(
    common: &ProviderCommonArgs,
) -> Result<provider_state::OwnershipModel> {
    let client = Client::new();
    load_ownership_model(&client, common).await
}

/// Fetch + extract provider ownership facts from the configured state source.
async fn load_facts(client: &Client, common: &ProviderCommonArgs) -> Result<Vec<provider_state::ProviderFact>> {
    if common.server.is_some() && common.facts_file.is_some() {
        bail!("--server and --facts-file are mutually exclusive");
    }
    if let Some(path) = &common.facts_file {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("reading facts file {}", path.display()))?;
        let records = parse_facts_content(&content)
            .with_context(|| format!("parsing facts file {}", path.display()))?;
        // Accept raw tier records (payload-string), decoded bodies, or bare
        // provider_facts — the tier extractor handles all three.
        return Ok(extract_and_report_coverage(&records, &format!("{}", path.display())));
    }
    if let Some(server) = &common.server {
        let mut url = format!("{}/api/ehdb/tiers/eventlog?limit=1000", server.trim_end_matches('/'));
        if let Some(exec) = &common.execution {
            url.push_str(&format!("&execution={}", exec));
        }
        let body: serde_json::Value = client
            .get(&url)
            .send()
            .await
            .with_context(|| format!("fetching eventlog tier from {url}"))?
            .error_for_status()
            .context("eventlog tier query returned an error status")?
            .json()
            .await
            .context("parsing eventlog tier response")?;
        // Wire shape: { outcome, result: { records: [EventLogRecordView, ...] } }.
        let empty = Vec::new();
        let records = body
            .get("result")
            .and_then(|r| r.get("records"))
            .and_then(|r| r.as_array())
            .unwrap_or(&empty);
        return Ok(extract_and_report_coverage(records, &url));
    }
    // No state source → empty model (everything untracked / import).
    Ok(Vec::new())
}

/// Extract provider facts and **say so on stderr** when the fold understood
/// nothing.
///
/// noetl/ai-meta#191's dangerous half is that an empty ownership model is a
/// legitimate state, so a caller cannot tell "nothing is tracked yet" from "the
/// extractor did not understand the data". noetl-tools now warns via `tracing`,
/// but this binary installs a subscriber **only for the `subscribe` subcommand**
/// (`src/subscribe/mod.rs`), so on the provider path that warning is emitted into
/// a void. The signal has to land in this command's own output or it does not
/// exist for the operator running it.
///
/// stderr, not stdout: `provider report --json` output must stay machine-parseable.
fn extract_and_report_coverage(
    records: &[serde_json::Value],
    source: &str,
) -> Vec<provider_state::ProviderFact> {
    let (facts, coverage) = provider_state::extract_facts_from_tier_records_with_coverage(records);
    if coverage.is_suspicious() {
        eprintln!(
            "warning: read {} record(s) from {source} and recognised none of them as \
             provider facts.\n         \
             This is a parse failure, not an empty ownership model — treating it as \
             \"nothing is tracked\"\n         \
             would make an adopt or a destroy plan against a world it cannot see. \
             (noetl/ai-meta#191)",
            coverage.considered
        );
    }
    facts
}

/// Choose the adopt target: the named step, or the sole provider step.
fn pick_target<'a>(resources: &'a [DeclaredResource], step: Option<&str>) -> Result<&'a DeclaredResource> {
    match step {
        Some(name) => resources
            .iter()
            .find(|r| r.step == name)
            .with_context(|| format!("no provider step named {name:?} in the playbook")),
        None => {
            if resources.len() == 1 {
                Ok(&resources[0])
            } else {
                bail!(
                    "playbook declares {} provider steps — pass --step to pick one",
                    resources.len()
                )
            }
        }
    }
}

/// Build a tools ExecutionContext, seeding `workload` variables for templates.
fn build_context(common: &ProviderCommonArgs) -> Result<ToolsExecutionContext> {
    // A stable, human-recognizable local execution id (not a real snowflake —
    // these CLI verbs run outside a playbook execution).
    let mut ctx = ToolsExecutionContext::new(0, "provider-cli", "");
    if let Some(w) = &common.workload {
        let val: serde_json::Value =
            serde_json::from_str(w).context("--workload must be a JSON object")?;
        ctx.set_variable("workload", val);
    }
    Ok(ctx)
}

fn is_mutating(tool: &Tool) -> bool {
    // Mutating ensure verbs end in `.ensure` / `.enable` / `.link` /
    // `.ensure_binding`; reads (`.list` / `.describe` / `.get_policy`) and
    // destroys are out of scope for plan/drift/orphans/adopt reconciliation.
    if let Tool::Provider { action, .. } = tool {
        let a = action.rsplit('.').next().unwrap_or("");
        matches!(a, "ensure" | "enable" | "link" | "ensure_binding")
    } else {
        false
    }
}

fn stack_of(common: &ProviderCommonArgs) -> String {
    common.stack.clone().unwrap_or_else(|| "<unscoped>".to_string())
}

fn bearer(token: String) -> AuthConfig {
    AuthConfig {
        auth_type: AuthType::Bearer,
        credential: None,
        token: Some(token),
        username: None,
        password: None,
        header: None,
        scopes: None,
    }
}

/// Set a key in a ToolConfig's config body (which is always an object here).
fn set(cfg: &mut ToolConfig, key: &str, value: serde_json::Value) {
    if let Some(obj) = cfg.config.as_object_mut() {
        obj.insert(key.to_string(), value);
    }
}

fn str_field(v: &serde_json::Value, key: &str) -> String {
    match v.get(key) {
        Some(serde_json::Value::String(s)) => s.clone(),
        Some(other) => other.to_string(),
        None => "-".to_string(),
    }
}

fn print_json(v: &serde_json::Value) {
    println!("{}", serde_json::to_string_pretty(v).unwrap_or_default());
}

#[cfg(test)]
mod facts_sink_tests {
    use super::*;

    fn tmp_path(tag: &str) -> PathBuf {
        std::env::temp_dir().join(format!("noetl-facts-{}-{}.jsonl", tag, std::process::id()))
    }

    #[test]
    fn append_then_read_back_round_trip_and_masking() {
        let path = tmp_path("rt");
        let _ = std::fs::remove_file(&path);

        // An applied adopt fact (ownership) + an applied billing fact whose
        // `desired` carries the billing-account id that must be masked.
        let adopt = serde_json::json!({
            "urn": "google:cloudresourcemanager:project:shastara",
            "provider": "google", "service": "cloudresourcemanager",
            "resource_type": "project", "verb": "adopt", "stack": "shastaratech-org-foundation",
            "outcome": "adopted", "execution_id": 1,
            "desired": { "project_id": "shastara", "display_name": "shastara" }
        });
        let billing = serde_json::json!({
            "urn": "google:cloudbilling:billing_link:shastaratech-youtube-prod",
            "provider": "google", "service": "cloudbilling",
            "resource_type": "billing_link", "verb": "ensure", "stack": "shastaratech-org-foundation",
            "outcome": "changed", "execution_id": 2,
            "desired": { "project_id": "shastaratech-youtube-prod", "billing_account": "billingAccounts/AAAAAA-BBBBBB-CCCCCC" }
        });
        // A planned (dry-run) fact that maybe_append_applied_fact must NOT write.
        let planned = serde_json::json!({
            "urn": "google:cloudresourcemanager:folder:20-media",
            "provider": "google", "service": "cloudresourcemanager",
            "resource_type": "folder", "verb": "ensure", "stack": "shastaratech-org-foundation",
            "outcome": "planned", "execution_id": 3, "desired": { "display_name": "20-media" }
        });

        append_fact_jsonl(&path, &adopt).unwrap();
        append_fact_jsonl(&path, &billing).unwrap();

        // Masking: the committed line must not carry the billing account id.
        let raw = std::fs::read_to_string(&path).unwrap();
        assert!(!raw.contains("AAAAAA-BBBBBB-CCCCCC"), "billing account id must be masked in the sink");
        assert!(raw.contains("<masked>"), "masked placeholder present");
        assert_eq!(raw.lines().count(), 2, "one JSONL line per appended fact");

        // Read back exactly as `--facts-file` does (JSONL) and fold → ownership.
        let records = parse_facts_content(&raw).unwrap();
        let facts = provider_state::extract_facts_from_tier_records(&records);
        let model = provider_state::fold_facts(&facts);
        assert!(
            model.owned.contains_key("google:cloudresourcemanager:project:shastara"),
            "adopted project is owned after the append→read-back round-trip"
        );
        assert!(
            model.owned.contains_key("google:cloudbilling:billing_link:shastaratech-youtube-prod"),
            "billing link is owned after round-trip (masking does not break the fold)"
        );

        // The planned/dry-run fact is filtered out by the apply-only gate.
        let planned_data = serde_json::json!({ "provider_fact": planned });
        let common = ProviderCommonArgs {
            playbook: path.clone(), stack: None, server: None,
            facts_file: None, facts_out: Some(path.clone()), execution: None,
            endpoint: None, workload: None, json: false,
        };
        maybe_append_applied_fact(&common, &planned_data).unwrap();
        let after = std::fs::read_to_string(&path).unwrap();
        assert_eq!(after.lines().count(), 2, "planned/dry-run fact must NOT be appended");

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn parse_facts_content_accepts_array_and_jsonl() {
        let array = r#"[{"urn":"u1","provider":"google","service":"s","resource_type":"project","verb":"ensure","stack":"x","outcome":"changed","execution_id":1,"desired":{}}]"#;
        assert_eq!(parse_facts_content(array).unwrap().len(), 1);
        let jsonl = "{\"a\":1}\n\n{\"b\":2}\n";
        assert_eq!(parse_facts_content(jsonl).unwrap().len(), 2, "JSONL: blank lines skipped");
    }
}

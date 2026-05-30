use anyhow::{Context, Result};
use duckdb::{params, Connection};
// `Deserialize` / `Serialize` derive macros come in through the
// `pub use noetl_executor::playbook::*` block below; no direct use
// in this file after R-1.1 PR-2b's extraction.  The `rhai::*` /
// `BufRead*` / `Arc` / `Mutex` imports left after R-1.1 PR-2c-3
// (rhai deletion) and PR-2c-4 (shell deletion) are scrubbed below;
// remaining tools still need them (PR-2c-5 onwards continues
// trimming).
#[allow(unused_imports)]
use serde::{Deserialize, Serialize};
use serde_yaml;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::process::Command;

// ---------------------------------------------------------------------------
// YAML playbook types (R-1.1 PR-2a, see Appendix H of the global hybrid
// cloud blueprint).
// ---------------------------------------------------------------------------
//
// The Pydantic-like types that used to live here (Playbook, Executor,
// Step, NextFormat, Tool, AuthConfig, SinkTarget, CmdsList, NextStep,
// LoopConfig, RuntimeCapabilities, and their impls) moved to
// `noetl-executor::playbook` so the same data model serves both the
// CLI's local-mode runner and the worker's NATS-mode runner (R-1.3).
//
// Field accessors and trait derives are preserved verbatim; the only
// difference is that types/fields that were private to this file are
// now `pub` in the executor crate so the impl below can reach them
// across the crate boundary.  No behaviour change.
//
// `#[allow(unused_imports)]` because some types (helpers used via
// `#[serde(default = "..")]` attributes inside the executor crate, and
// types not referenced by name in this file's impl block) get pulled in
// for completeness.  R-1.1 PR-2b extracts the parser logic and most of
// these become actively used; for now the allow keeps the re-export
// glob clean and the impl below stable.
#[allow(unused_imports)]
pub use noetl_executor::playbook::{
    default_duckdb_path, default_method, default_profile, default_version, AuthConfig,
    CaseCondition, CmdsList, Executor, ExecutorRequires, ExecutorSpec, LoopConfig, Metadata,
    NextArc, NextFormat, NextMode, NextRouterSpec, NextStep, Playbook, RuntimeCapabilities,
    SinkFormat, SinkTarget, Step, StepSpec, ThenBlock, Tool, WhenCondition,
};


/// Structured outcome of a local playbook run.
///
/// Captures status, per-step results, timing, and the final result (last
/// step's output). Designed to be serialized as JSON for programmatic
/// consumers (the noetl ↔ Codex bridge being the canonical example —
/// it pipes this envelope into a downstream `jq` filter and from there
/// into Claude's read of `outbox/{id}.result.json`).
///
/// Two output modes:
/// - **Human progress** is always written to *stderr* (eprintln) so it
///   doesn't pollute the JSON-on-stdout pipe. With `--quiet` even those
///   diagnostic lines are suppressed.
/// - **Structured outcome** is written to *stdout* as JSON when
///   `emit_json` is set. Without it, `run()` is silent on stdout.
#[derive(Debug, serde::Serialize, serde::Deserialize, Default)]
pub struct RunOutcome {
    pub status: String,         // "ok" | "error"
    pub playbook_name: String,
    pub playbook_path: String,
    pub started_at: String,     // RFC3339 UTC
    pub completed_at: String,
    pub duration_seconds: f64,
    pub executed_steps: Vec<String>,
    pub step_results: std::collections::BTreeMap<String, String>,
    pub final_result: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

pub struct PlaybookRunner {
    playbook_path: PathBuf,
    variables: HashMap<String, String>,
    verbose: bool,
    target: Option<String>,
    merge: bool,
    /// When true, suppress all human-readable progress output (even
    /// stderr).  JSON output on stdout via `emit_json` is unaffected.
    quiet: bool,
    /// When true, after `run()` completes the runner serialises a
    /// `RunOutcome` to stdout as JSON. Combined with `quiet=true` this
    /// gives a pipeline-friendly invocation:
    ///     noetl exec --runtime local foo.yaml --json
    /// → progress on stderr (or nothing in --quiet), structured JSON
    /// envelope on stdout.
    emit_json: bool,
}

impl PlaybookRunner {
    pub fn new(playbook_path: PathBuf) -> Self {
        Self {
            playbook_path,
            variables: HashMap::new(),
            verbose: false,
            target: None,
            merge: false,
            quiet: false,
            emit_json: false,
        }
    }

    pub fn with_variables(mut self, vars: HashMap<String, String>) -> Self {
        self.variables = vars;
        self
    }

    pub fn with_merge(mut self, merge: bool) -> Self {
        self.merge = merge;
        self
    }

    pub fn with_verbose(mut self, verbose: bool) -> Self {
        self.verbose = verbose;
        self
    }

    pub fn with_target(mut self, target: Option<String>) -> Self {
        self.target = target;
        self
    }

    pub fn with_quiet(mut self, quiet: bool) -> Self {
        self.quiet = quiet;
        self
    }

    pub fn with_emit_json(mut self, emit_json: bool) -> Self {
        self.emit_json = emit_json;
        self
    }

    // NOTE: an earlier draft of this commit added a `say()` helper
    // that gated all stderr prints on `!self.quiet`. Inlining the
    // check at each call site turned out simpler (and avoided the
    // dead-code warning when callers used `eprintln!` directly).
    // Kept as a comment marker for future contributors who might
    // re-introduce the helper.

    /// Validate playbook requirements against local runtime capabilities
    fn validate_capabilities(&self, playbook: &Playbook) -> Result<()> {
        // R-1.1 PR-2b: validation logic extracted to
        // noetl_executor::capabilities::validate_capabilities.  The
        // pure function returns a ValidationReport; this CLI
        // adapter formats the report against the CLI's playbook_path
        // for human-readable error messages.
        use noetl_executor::capabilities::{validate_capabilities, ValidationError};

        let local_caps = RuntimeCapabilities::local();
        let report = validate_capabilities(playbook, &local_caps)?;

        // CLI-side: warnings go to stderr.
        for warning in &report.warnings {
            eprintln!("Warning: {}", warning);
        }

        // CLI-side: first error short-circuits with a formatted message
        // (matches pre-extraction behaviour).
        if let Some(err) = report.errors.first() {
            match err {
                ValidationError::IncompatibleProfile { required } => {
                    anyhow::bail!(
                        "Playbook '{}' requires {} runtime (executor.profile: {})\n\
                         Use: noetl exec {} --runtime {}",
                        playbook.metadata.name,
                        required,
                        required,
                        self.playbook_path.display(),
                        required,
                    );
                }
                ValidationError::MissingTool { tool, supported } => {
                    anyhow::bail!(
                        "Playbook '{}' requires tool '{}' which is not supported by local runtime.\n\
                         Supported tools: {:?}\n\
                         Consider using: noetl exec {} --runtime distributed",
                        playbook.metadata.name,
                        tool,
                        supported,
                        self.playbook_path.display(),
                    );
                }
                ValidationError::MissingFeature { feature, supported } => {
                    anyhow::bail!(
                        "Playbook '{}' requires feature '{}' which is not supported by local runtime.\n\
                         Supported features: {:?}\n\
                         Consider using: noetl exec {} --runtime distributed",
                        playbook.metadata.name,
                        feature,
                        supported,
                        self.playbook_path.display(),
                    );
                }
            }
        }

        Ok(())
    }

    pub fn run(&self) -> Result<RunOutcome> {
        // RFC3339 UTC timestamp for the outcome envelope. Manual format
        // because chrono's `to_rfc3339_opts` is overkill for one line.
        let started_at = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
        let started_instant = std::time::Instant::now();

        // Load and parse playbook
        let content = fs::read_to_string(&self.playbook_path).context("Failed to read playbook file")?;

        let playbook: Playbook = serde_yaml::from_str(&content).context("Failed to parse playbook YAML")?;

        // Validate playbook against local runtime capabilities
        self.validate_capabilities(&playbook)?;

        if !self.quiet {
            eprintln!("📋 Running playbook: {}", playbook.metadata.name);
            eprintln!("   API Version: {}", playbook.api_version);
        }

        if let Some(executor) = &playbook.executor {
            if self.verbose {
                eprintln!("   Executor Profile: {}", executor.profile);
                eprintln!("   Executor Version: {}", executor.version);
            }
        }

        // Initialize execution context with workload variables
        let mut context = ExecutionContext::new();
        if let Some(workload) = &playbook.workload {
            for (key, value) in workload {
                // Convert YAML value to plain string (not YAML formatted)
                let value_str = match value {
                    serde_yaml::Value::String(s) => s.clone(),
                    serde_yaml::Value::Number(n) => n.to_string(),
                    serde_yaml::Value::Bool(b) => b.to_string(),
                    other => serde_yaml::to_string(other)?.trim().to_string(),
                };
                context.set_variable(format!("workload.{}", key), value_str);
            }
        }

        // Add user-provided variables
        // By default (merge=false), user variables override workload variables (shallow merge)
        // With merge=true, we would do deep merge (but for now, we only support shallow)
        for (key, value) in &self.variables {
            // If key doesn't have workload prefix, add it for consistency with API
            let var_key = if key.starts_with("workload.") {
                key.clone()
            } else {
                // Set both with and without workload prefix for compatibility
                context.set_variable(key.clone(), value.clone());
                format!("workload.{}", key)
            };
            context.set_variable(var_key, value.clone());
        }

        // Determine starting step using canonical rules:
        // 1. Command-line target overrides everything
        // 2. executor.spec.entry_step if configured
        // 3. Default: workflow[0].step (first step in workflow array)
        let starting_step = if let Some(target) = &self.target {
            target.clone()
        } else if let Some(executor) = &playbook.executor {
            if let Some(spec) = &executor.spec {
                if let Some(entry) = &spec.entry_step {
                    entry.clone()
                } else {
                    // Default to first workflow step
                    playbook
                        .workflow
                        .first()
                        .map(|s| s.step.clone())
                        .unwrap_or_else(|| "start".to_string())
                }
            } else {
                // Default to first workflow step
                playbook
                    .workflow
                    .first()
                    .map(|s| s.step.clone())
                    .unwrap_or_else(|| "start".to_string())
            }
        } else {
            // Default to first workflow step
            playbook
                .workflow
                .first()
                .map(|s| s.step.clone())
                .unwrap_or_else(|| "start".to_string())
        };

        if self.target.is_some() {
            eprintln!("🎯 Target: {}", starting_step);
        }

        // Track final_step for post-quiescence execution
        let final_step = playbook
            .executor
            .as_ref()
            .and_then(|e| e.spec.as_ref())
            .and_then(|s| s.final_step.clone());

        // Execute workflow starting from the entry step
        self.execute_step(&playbook, &starting_step, &mut context)?;

        // Execute final_step if configured and not already executed
        if let Some(final_step_name) = &final_step {
            if final_step_name != &starting_step {
                if self.verbose {
                    eprintln!("\n📍 Running final step: {}", final_step_name);
                }
                self.execute_step(&playbook, final_step_name, &mut context)?;
            }
        }

        if self.verbose && !self.quiet {
            eprintln!("✅ Playbook execution completed successfully");
        }

        // Build the structured outcome from the execution context.
        let completed_at = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
        let duration = started_instant.elapsed().as_secs_f64();

        // Snapshot the step results in the order they executed. The
        // ExecutionContext's HashMap ordering isn't stable, so we
        // also keep a separate Vec<String> of executed step names.
        let executed_steps = context.executed_steps.clone();
        let mut step_results: std::collections::BTreeMap<String, String> =
            std::collections::BTreeMap::new();
        for (name, value) in context.step_results.iter() {
            step_results.insert(name.clone(), value.clone());
        }

        // The "final result" convention for the local runtime: take
        // the last executed step's result, if any. Distributed runtime
        // has a richer notion (the final_step's output, possibly
        // transformed); local runtime keeps it simple.
        let final_result = executed_steps
            .last()
            .and_then(|name| step_results.get(name).cloned());

        let outcome = RunOutcome {
            status: "ok".to_string(),
            playbook_name: playbook.metadata.name.clone(),
            playbook_path: self.playbook_path.display().to_string(),
            started_at,
            completed_at,
            duration_seconds: duration,
            executed_steps,
            step_results,
            final_result,
            error: None,
        };

        if self.emit_json {
            // Pretty-printed for human readability when piped through `jq`;
            // structurally identical to compact form. stdout is reserved
            // for this single envelope so the caller's `>` redirect captures
            // exactly the JSON.
            match serde_json::to_string_pretty(&outcome) {
                Ok(json) => println!("{}", json),
                Err(e) => {
                    // Should never happen for our struct; fall back to
                    // a minimal error envelope so callers always see JSON.
                    eprintln!("Failed to serialise RunOutcome: {}", e);
                    println!(
                        r#"{{"status":"error","error":"failed to serialise RunOutcome: {}"}}"#,
                        e
                    );
                }
            }
        }

        Ok(outcome)
    }

    fn execute_step(&self, playbook: &Playbook, step_name: &str, context: &mut ExecutionContext) -> Result<()> {
        // Find the step
        let step = playbook
            .workflow
            .iter()
            .find(|s| s.step == step_name)
            .context(format!("Step '{}' not found", step_name))?;

        // Terminal "end" step - no-op for backwards compatibility
        if step_name == "end" {
            return Ok(());
        }

        // Evaluate step.when enablement guard (canonical v2)
        if let Some(when_guard) = &step.when_guard {
            // Render template first, then evaluate
            let rendered_guard = self.render_template(when_guard, context)?;
            let is_enabled = self.evaluate_condition(&rendered_guard, context)?;

            if !is_enabled {
                if self.verbose {
                    eprintln!("\n⏭️  Step '{}' skipped (when guard: {})", step_name, when_guard);
                }
                // Step is disabled - do not execute, branch terminates here
                return Ok(());
            }
        }

        eprintln!("\n🔹 Step: {}", step_name);
        if let Some(desc) = &step.desc {
            eprintln!("   Description: {}", desc);
        }

        // DSL v2: Process step.input and merge into context as input.* variables
        if let Some(input_map) = &step.input {
            for (key, value_yaml) in input_map {
                let template = match value_yaml {
                    serde_yaml::Value::String(s) => s.clone(),
                    serde_yaml::Value::Number(n) => n.to_string(),
                    serde_yaml::Value::Bool(b) => b.to_string(),
                    other => serde_yaml::to_string(other)?.trim().to_string(),
                };
                let value = self.render_template(&template, context)?;
                context.set_variable(format!("input.{}", key), value);
            }
        }

        // Execute the tool and capture result
        let step_result: Option<String> = if let Some(tool) = &step.tool {
            let result = self.execute_tool(tool, context)?;

            // Store step result for reference in templates
            if let Some(result_json) = &result {
                context.set_step_result(step_name.to_string(), result_json.clone());
            }
            result
        } else {
            None
        };

        // Handle vars extraction - make result available as "result" for JSON path access
        if let Some(vars) = &step.vars {
            // Parse step result as JSON for path access
            let result_json: Option<serde_json::Value> =
                step_result.as_ref().and_then(|s| serde_json::from_str(s).ok());

            for (key, template) in vars {
                // Handle result.* JSON path expressions
                let value = if template.contains("result.") || template.contains("result[") {
                    self.render_template_with_result(template, context, result_json.as_ref())?
                } else {
                    self.render_template(template, context)?
                };
                context.set_variable(format!("vars.{}", key), value);
            }
        }

        // Handle case conditions (evaluate before next)
        let mut case_matched = false;
        if let Some(cases) = &step.case {
            if self.verbose {
                eprintln!("   Evaluating {} case conditions...", cases.len());
            }
            for case in cases {
                let (condition_result, condition_display) = match &case.when {
                    WhenCondition::Rhai { rhai } => {
                        // Evaluate Rhai expression
                        let result = self.evaluate_rhai_condition(rhai, context)?;
                        (
                            result,
                            format!("rhai: {}...", &rhai.chars().take(40).collect::<String>()),
                        )
                    }
                    WhenCondition::Simple { when } => {
                        // Render template first, then evaluate condition
                        let rendered_condition = self.render_template(when, context)?;
                        let result = self.evaluate_condition(&rendered_condition, context)?;
                        (result, when.clone())
                    }
                };

                if self.verbose && !condition_result {
                    eprintln!("   ✗ {}", condition_display);
                }

                if condition_result {
                    case_matched = true;
                    if self.verbose {
                        eprintln!("   ✓ Condition matched: {}", condition_display);
                    }

                    // Execute then steps (potentially in parallel if multiple)
                    // Handle both list and single-object formats for then block
                    match &case.then {
                        ThenBlock::List(steps) => {
                            self.execute_next_steps(playbook, steps, context)?;
                        }
                        ThenBlock::Single(value) => {
                            // For single object format, try to extract next steps
                            // This handles the dict format: then: { next: ... }
                            if let Some(next_val) = value.get("next") {
                                if let Ok(steps) = serde_yaml::from_value::<Vec<NextStep>>(next_val.clone()) {
                                    self.execute_next_steps(playbook, &steps, context)?;
                                } else if let Ok(step) = serde_yaml::from_value::<NextStep>(next_val.clone()) {
                                    self.execute_next_steps(playbook, &[step], context)?;
                                }
                            }
                            // Handle pipe: blocks - pass to distributed executor
                            // For local CLI, we skip pipeline execution (requires distributed runtime)
                            if value.get("pipe").is_some() {
                                if self.verbose {
                                    eprintln!("   ⚠ Pipeline blocks require distributed runtime, skipping");
                                }
                            }
                        }
                    }
                    break;
                } else if let Some(else_steps) = &case.else_steps {
                    if self.verbose {
                        eprintln!("   ✗ Condition not matched, executing else branch");
                    }
                    self.execute_next_steps(playbook, else_steps, context)?;
                    break;
                }
            }
        }

        // Execute next steps only if no case matched or no case defined
        if !case_matched {
            if let Some(next_value) = &step.next {
                // Parse next as either v10 router format or legacy array format
                if let Some(next_format) = NextFormat::from_yaml_value(next_value) {
                    match next_format {
                        NextFormat::Router { spec, arcs } => {
                            // V10 router format: get mode from router.spec
                            let next_mode = spec
                                .as_ref()
                                .and_then(|s| s.mode.as_ref())
                                .map(|m| {
                                    if m == "inclusive" {
                                        NextMode::Inclusive
                                    } else {
                                        NextMode::Exclusive
                                    }
                                })
                                .unwrap_or(NextMode::Exclusive);

                            self.execute_router_arcs(playbook, &arcs, context, &next_mode)?;
                        }
                        NextFormat::Array(next_steps) => {
                            // Legacy array format: get next_mode from step spec
                            let next_mode = step
                                .spec
                                .as_ref()
                                .and_then(|s| s.next_mode.clone())
                                .unwrap_or(NextMode::Exclusive);

                            self.execute_next_steps_with_mode(playbook, &next_steps, context, &next_mode)?;
                        }
                    }
                }
            }
            // No next section = branch termination (leaf step)
        }

        Ok(())
    }

    /// Execute next steps with canonical routing semantics
    /// next_mode: exclusive (first match) or inclusive (all matches)
    fn execute_next_steps(
        &self,
        playbook: &Playbook,
        next_steps: &[NextStep],
        context: &mut ExecutionContext,
    ) -> Result<()> {
        self.execute_next_steps_with_mode(playbook, next_steps, context, &NextMode::Exclusive)
    }

    /// Execute next steps with specified routing mode
    fn execute_next_steps_with_mode(
        &self,
        playbook: &Playbook,
        next_steps: &[NextStep],
        context: &mut ExecutionContext,
        next_mode: &NextMode,
    ) -> Result<()> {
        let mut matched_steps: Vec<String> = Vec::new();
        let mut matched_args: Vec<Option<HashMap<String, serde_yaml::Value>>> = Vec::new();

        // Evaluate conditions and collect matching steps
        for next in next_steps {
            match next {
                NextStep::Canonical {
                    step,
                    when_condition,
                    args,
                } => {
                    // Canonical v2 format: evaluate when condition if present
                    let matches = if let Some(condition) = when_condition {
                        let rendered = self.render_template(condition, context)?;
                        self.evaluate_condition(&rendered, context)?
                    } else {
                        // No when condition = always matches (default arc)
                        true
                    };

                    if matches {
                        if self.verbose {
                            if let Some(cond) = when_condition {
                                eprintln!("   ✓ Route matched: {} ({})", step, cond);
                            } else {
                                eprintln!("   ✓ Route: {} (default)", step);
                            }
                        }

                        matched_steps.push(step.clone());
                        matched_args.push(args.clone());

                        // In exclusive mode, stop at first match
                        if matches!(next_mode, NextMode::Exclusive) {
                            break;
                        }
                    } else if self.verbose {
                        if let Some(cond) = when_condition {
                            eprintln!("   ✗ Route skipped: {} ({})", step, cond);
                        }
                    }
                }
                NextStep::Conditional { when, then } => {
                    // Legacy conditional format
                    if let Some(condition) = when {
                        let rendered = self.render_template(condition, context)?;
                        if self.evaluate_condition(&rendered, context)? {
                            self.execute_next_steps_with_mode(playbook, then, context, next_mode)?;
                            if matches!(next_mode, NextMode::Exclusive) {
                                return Ok(());
                            }
                        }
                    }
                }
                NextStep::NextAction { next } => {
                    // Legacy { next: [...] } format
                    self.execute_next_steps_with_mode(playbook, next, context, next_mode)?;
                    return Ok(());
                }
            }
        }

        // Branch termination: no matches = branch ends
        if matched_steps.is_empty() {
            if self.verbose && !next_steps.is_empty() {
                eprintln!("   ⏹️  Branch terminated (no matching routes)");
            }
            return Ok(());
        }

        // Log fan-out in inclusive mode
        if matches!(next_mode, NextMode::Inclusive) && matched_steps.len() > 1 && self.verbose {
            eprintln!("   ⚡ Fan-out to {} steps: {:?}", matched_steps.len(), matched_steps);
        }

        // Execute matched steps
        for (i, step_name) in matched_steps.iter().enumerate() {
            // Apply args to context if present
            if let Some(Some(args)) = matched_args.get(i) {
                for (key, value) in args {
                    let value_str = match value {
                        serde_yaml::Value::String(s) => s.clone(),
                        serde_yaml::Value::Number(n) => n.to_string(),
                        serde_yaml::Value::Bool(b) => b.to_string(),
                        other => serde_yaml::to_string(other)?.trim().to_string(),
                    };
                    context.set_variable(format!("args.{}", key), value_str);
                }
            }

            self.execute_step(playbook, step_name, context)?;
        }

        Ok(())
    }

    /// Execute v10 router arcs with specified routing mode
    fn execute_router_arcs(
        &self,
        playbook: &Playbook,
        arcs: &[NextArc],
        context: &mut ExecutionContext,
        next_mode: &NextMode,
    ) -> Result<()> {
        let mut matched_steps: Vec<String> = Vec::new();
        let mut matched_args: Vec<Option<HashMap<String, serde_yaml::Value>>> = Vec::new();

        // Evaluate conditions and collect matching arcs
        for arc in arcs {
            // Evaluate when condition if present
            let matches = if let Some(condition) = &arc.when_condition {
                let rendered = self.render_template(condition, context)?;
                self.evaluate_condition(&rendered, context)?
            } else {
                // No when condition = always matches (default arc)
                true
            };

            if matches {
                if self.verbose {
                    if let Some(cond) = &arc.when_condition {
                        eprintln!("   ✓ Arc matched: {} ({})", arc.step, cond);
                    } else {
                        eprintln!("   ✓ Arc: {} (default)", arc.step);
                    }
                }

                matched_steps.push(arc.step.clone());
                matched_args.push(arc.args.clone());

                // In exclusive mode, stop at first match
                if matches!(next_mode, NextMode::Exclusive) {
                    break;
                }
            } else if self.verbose {
                if let Some(cond) = &arc.when_condition {
                    eprintln!("   ✗ Arc skipped: {} ({})", arc.step, cond);
                }
            }
        }

        // Branch termination: no matches = branch ends
        if matched_steps.is_empty() {
            if self.verbose && !arcs.is_empty() {
                eprintln!("   ⏹️  Branch terminated (no matching arcs)");
            }
            return Ok(());
        }

        // Log fan-out in inclusive mode
        if matches!(next_mode, NextMode::Inclusive) && matched_steps.len() > 1 && self.verbose {
            eprintln!("   ⚡ Fan-out to {} steps: {:?}", matched_steps.len(), matched_steps);
        }

        // Execute matched steps
        for (i, step_name) in matched_steps.iter().enumerate() {
            // Apply args to context if present
            if let Some(Some(args)) = matched_args.get(i) {
                for (key, value) in args {
                    let value_str = match value {
                        serde_yaml::Value::String(s) => s.clone(),
                        serde_yaml::Value::Number(n) => n.to_string(),
                        serde_yaml::Value::Bool(b) => b.to_string(),
                        other => serde_yaml::to_string(other)?.trim().to_string(),
                    };
                    context.set_variable(format!("args.{}", key), value_str);
                }
            }

            self.execute_step(playbook, step_name, context)?;
        }

        Ok(())
    }

    fn evaluate_condition(&self, condition: &str, context: &ExecutionContext) -> Result<bool> {
        // R-1.1 PR-2b: body extracted to noetl_executor::condition.
        noetl_executor::condition::evaluate_condition(condition, &context.variables)
    }

    /// Evaluate a Rhai expression as a boolean condition
    /// The Rhai code should return a boolean (true/false)
    fn evaluate_rhai_condition(&self, code: &str, context: &ExecutionContext) -> Result<bool> {
        // R-1.1 PR-2b: body extracted to noetl_executor::condition.
        noetl_executor::condition::evaluate_rhai_condition(code, &context.variables)
    }

    fn execute_tool(&self, tool: &Tool, context: &mut ExecutionContext) -> Result<Option<String>> {
        match tool {
            Tool::Shell { cmds } => {
                // R-1.1 PR-2c-4: dispatch through the noetl-tools
                // bridge instead of CLI's inline execute_shell_command.
                // Per-command bash invocations preserved; bridge
                // returns the LAST command's stdout for the step
                // result (matches existing CLI contract).
                //
                // Semantic note: noetl-tools' ShellTool collects
                // stdout + stderr and returns them at completion.
                // The CLI's pre-PR-2c-4 implementation streamed
                // output to the terminal line-by-line.  Long-running
                // shell steps no longer show real-time output;
                // documented in the PR body and on the
                // executor-crate-architecture wiki page.

                // Render each command's template first so the bridge
                // dispatch runs the rendered commands (not the raw
                // templates).
                let rendered_cmds = match cmds {
                    CmdsList::Single(cmd) => CmdsList::Single(self.render_template(cmd, context)?),
                    CmdsList::Multiple(c) => {
                        let mut out = Vec::with_capacity(c.len());
                        for raw in c {
                            out.push(self.render_template(raw, context)?);
                        }
                        CmdsList::Multiple(out)
                    }
                };

                let rendered_tool = Tool::Shell {
                    cmds: rendered_cmds,
                };
                let bridge_ctx = noetl_executor::tools_bridge::BridgeContext {
                    execution_id: 0,
                    step: "<cli-local>",
                    variables: &context.variables,
                    server_url: String::new(),
                    worker_id: None,
                    command_id: None,
                };
                let outcome = tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current().block_on(
                        noetl_executor::tools_bridge::dispatch_via_registry(
                            &rendered_tool,
                            &bridge_ctx,
                        ),
                    )
                })?;
                Ok(outcome.result)
            }
            Tool::Http {
                method,
                url,
                headers,
                params,
                body,
                auth,
            } => {
                // R-1.1 PR-2c-5: dispatch through the noetl-tools
                // bridge instead of CLI's inline execute_http_request.
                // The bridge:
                //   - resolves CLI's `AuthConfig` to a Bearer token
                //     via noetl-tools' `GcpAuth` (replaces the
                //     `gcloud auth print-access-token` shellout);
                //   - issues the HTTP request via `reqwest` (replaces
                //     the `curl` subprocess);
                //   - reshapes noetl-tools' `{status_code, headers,
                //     body}` envelope to the CLI's pre-PR-2c-5
                //     `{status, body}` shape so playbook steps can
                //     keep branching on `<step>.body.status`.
                //
                // Templates (url, headers, params, body) are rendered
                // here against the CLI's HashMap<String, String>
                // context BEFORE handing the rendered tool to the
                // bridge, so the bridge dispatches against fully
                // expanded values.
                let rendered_url = self.render_template(url, context)?;

                if self.verbose {
                    eprintln!("   HTTP {} {}", method, rendered_url);
                }

                let mut rendered_headers = HashMap::with_capacity(headers.len());
                for (k, v) in headers {
                    rendered_headers.insert(k.clone(), self.render_template(v, context)?);
                }
                let mut rendered_params = HashMap::with_capacity(params.len());
                for (k, v) in params {
                    rendered_params.insert(k.clone(), self.render_template(v, context)?);
                }
                let rendered_body = if let Some(b) = body {
                    Some(self.render_template(b, context)?)
                } else {
                    None
                };

                let rendered_tool = Tool::Http {
                    method: method.clone(),
                    url: rendered_url,
                    headers: rendered_headers,
                    params: rendered_params,
                    body: rendered_body,
                    auth: auth.clone(),
                };
                let bridge_ctx = noetl_executor::tools_bridge::BridgeContext {
                    execution_id: 0,
                    step: "<cli-local>",
                    variables: &context.variables,
                    server_url: String::new(),
                    worker_id: None,
                    command_id: None,
                };
                let outcome = tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current().block_on(
                        noetl_executor::tools_bridge::dispatch_via_registry(
                            &rendered_tool,
                            &bridge_ctx,
                        ),
                    )
                })?;
                if self.verbose {
                    if let Some(ref r) = outcome.result {
                        let preview = if r.len() > 200 {
                            format!("{}... ({} bytes)", &r[..200], r.len())
                        } else {
                            r.clone()
                        };
                        eprintln!("   Response: {}", preview);
                    }
                }
                Ok(outcome.result)
            }
            Tool::Playbook { path, args, input } => {
                // R-1.1 PR-2c-7: per § H.10, `Tool::Playbook` is
                // the recursion case of the CLI's tree walker —
                // the sub-playbook is dispatched through another
                // `PlaybookRunner` in-process, not through the
                // noetl-tools registry.  The bridge dispatch arm
                // bails loudly if anyone tries to route this kind
                // through it.
                //
                // The variable-preparation step (merging the
                // parent context with DSL v2 `input:` or DSL v1
                // `args:`, each rendered against the parent and
                // prefixed with `workload.`) DID move into the
                // executor as
                // `noetl_executor::tools_bridge::prepare_sub_playbook_vars`
                // so future callers (and unit tests) can reuse it.
                let rendered_path = self.render_template(path, context)?;
                let playbook_path = self.resolve_playbook_path(&rendered_path)?;

                if self.verbose {
                    eprintln!("   Executing sub-playbook: {}", playbook_path.display());
                }

                let sub_vars = noetl_executor::tools_bridge::prepare_sub_playbook_vars(
                    &context.variables,
                    args,
                    input,
                    |t| self.render_template(t, context),
                )?;

                // Propagate quiet to sub-playbooks so they're consistently
                // silent; deliberately do NOT propagate emit_json — we
                // only want one structured envelope (the top-level run's)
                // on stdout. Sub-playbook outcomes are folded into the
                // parent's step_results via the caller's code below.
                let sub_runner = PlaybookRunner::new(playbook_path)
                    .with_variables(sub_vars)
                    .with_verbose(self.verbose)
                    .with_quiet(self.quiet);
                let _sub_outcome = sub_runner.run()?;

                Ok(None)
            }
            Tool::DuckDb { db, query, params } => {
                // R-1.1 PR-2c-6: dispatch through the noetl-tools
                // bridge instead of CLI's inline execute_duckdb_query.
                //
                // The CLI keeps owning:
                //  - playbook-relative path resolution
                //    (`resolve_duckdb_path`), and
                //  - the parent-directory `mkdir -p` step that the
                //    bridge does NOT replicate (noetl-tools' DuckdbTool
                //    just opens the path as-given).
                //
                // The bridge owns the query execution + result
                // reshape so the CLI's SELECT/non-SELECT envelope
                // shape is preserved.
                let rendered_db = self.render_template(db, context)?;
                let db_path = self.resolve_duckdb_path(&rendered_db)?;

                if self.verbose {
                    eprintln!("   DuckDB: {}", db_path.display());
                }

                let query_str = match query {
                    Some(q) => q,
                    None => return Ok(None),
                };
                let rendered_query = self.render_template(query_str, context)?;
                let rendered_params: Vec<String> = params
                    .iter()
                    .map(|p| self.render_template(p, context))
                    .collect::<Result<Vec<_>>>()?;

                // Ensure parent directory exists — matches the CLI's
                // pre-PR-2c-6 behaviour where `execute_duckdb_query`
                // ran `fs::create_dir_all(parent)` before opening
                // the connection.
                if let Some(parent) = db_path.parent() {
                    fs::create_dir_all(parent)?;
                }

                let db_path_str = db_path
                    .to_str()
                    .context("DuckDB path is not valid UTF-8")?
                    .to_string();
                let rendered_tool = Tool::DuckDb {
                    db: db_path_str,
                    query: Some(rendered_query),
                    params: rendered_params,
                };
                let bridge_ctx = noetl_executor::tools_bridge::BridgeContext {
                    execution_id: 0,
                    step: "<cli-local>",
                    variables: &context.variables,
                    server_url: String::new(),
                    worker_id: None,
                    command_id: None,
                };
                let outcome = tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current().block_on(
                        noetl_executor::tools_bridge::dispatch_via_registry(
                            &rendered_tool,
                            &bridge_ctx,
                        ),
                    )
                })?;
                Ok(outcome.result)
            }
            Tool::Auth {
                provider,
                scopes,
                project,
            } => {
                if self.verbose {
                    eprintln!("   Auth: provider={}", provider);
                }

                // Set project in context if provided
                if let Some(proj) = project {
                    let rendered_project = self.render_template(proj, context)?;
                    context.set_variable("auth.project".to_string(), rendered_project);
                }

                // R-1.1 PR-2c-5: resolve via the same bridge helper
                // Tool::Http uses, so both paths share the
                // noetl-tools GcpAuth provider (gcloud shellout →
                // gcp_auth crate).  PR-2c-8 will move Tool::Auth's
                // full dispatch through the registry; for now we
                // unify just the credential resolution step.
                let auth_cfg = noetl_executor::playbook::AuthConfig {
                    provider: provider.clone(),
                    scopes: scopes.clone(),
                };
                let token = tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current().block_on(
                        noetl_executor::tools_bridge::resolve_auth_to_bearer(&auth_cfg),
                    )
                })?;

                // Store token in context for subsequent HTTP calls
                context.set_variable("auth.token".to_string(), token.clone());
                context.set_variable("auth.provider".to_string(), provider.clone());

                Ok(Some(token))
            }
            Tool::Sink { target, format } => {
                // Get the last step result to sink
                let data = context.step_results.values().last().cloned().unwrap_or_default();

                let formatted_data = match format {
                    SinkFormat::Json => data.clone(),
                    SinkFormat::Yaml => {
                        // Convert JSON to YAML if possible
                        if let Ok(json_val) = serde_json::from_str::<serde_json::Value>(&data) {
                            serde_yaml::to_string(&json_val).unwrap_or(data.clone())
                        } else {
                            data.clone()
                        }
                    }
                    SinkFormat::Csv => {
                        // Basic JSON array to CSV conversion
                        self.json_to_csv(&data)?
                    }
                };

                match target {
                    SinkTarget::File { path } => {
                        let rendered_path = self.render_template(path, context)?;
                        let file_path = self.resolve_sink_path(&rendered_path)?;

                        if self.verbose {
                            eprintln!("   Sink to file: {}", file_path.display());
                        }

                        // Create parent directories if needed
                        if let Some(parent) = file_path.parent() {
                            fs::create_dir_all(parent)?;
                        }
                        fs::write(&file_path, &formatted_data)?;
                        Ok(Some(format!("Written to {}", file_path.display())))
                    }
                    SinkTarget::DuckDb { db, table } => {
                        let rendered_db = self.render_template(db, context)?;
                        let rendered_table = self.render_template(table, context)?;
                        let db_path = self.resolve_duckdb_path(&rendered_db)?;

                        if self.verbose {
                            eprintln!("   Sink to DuckDB: {} -> {}", db_path.display(), rendered_table);
                        }

                        self.sink_to_duckdb(&db_path, &rendered_table, &data)?;
                        Ok(Some(format!("Inserted into {}", rendered_table)))
                    }
                    SinkTarget::Gcs { bucket, path } => {
                        let rendered_bucket = self.render_template(bucket, context)?;
                        let rendered_path = self.render_template(path, context)?;
                        let gcs_uri = format!("gs://{}/{}", rendered_bucket, rendered_path);

                        if self.verbose {
                            eprintln!("   Sink to GCS: {}", gcs_uri);
                        }

                        self.sink_to_gcs(&gcs_uri, &formatted_data)?;
                        Ok(Some(format!("Uploaded to {}", gcs_uri)))
                    }
                }
            }
            Tool::Rhai { code, args } => {
                // Render templates in args + code (same as pre-R-1.1
                // behaviour — the bridge runs against rendered input).
                let mut rendered_args: HashMap<String, String> = HashMap::new();
                for (key, template) in args {
                    let value = self.render_template(template, context)?;
                    rendered_args.insert(key.clone(), value);
                }
                let rendered_code = self.render_template(code, context)?;

                if self.verbose {
                    eprintln!("   🦀 Executing Rhai script");
                }

                // R-1.1 PR-2c-3: dispatch through the noetl-tools
                // bridge instead of the CLI's inline execute_rhai_script.
                // The async bridge dispatch is invoked from this sync
                // function via block_in_place + Handle::current().
                let rendered_tool = Tool::Rhai {
                    code: rendered_code,
                    args: rendered_args,
                };
                let bridge_ctx = noetl_executor::tools_bridge::BridgeContext {
                    execution_id: 0, // CLI local mode doesn't carry a snowflake id
                    step: "<cli-local>",
                    variables: &context.variables,
                    server_url: String::new(),
                    worker_id: None,
                    command_id: None,
                };
                let outcome = tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current().block_on(
                        noetl_executor::tools_bridge::dispatch_via_registry(
                            &rendered_tool,
                            &bridge_ctx,
                        ),
                    )
                })?;
                Ok(outcome.result)
            }
            Tool::Unsupported => {
                eprintln!("   Tool not supported in local execution mode");
                eprintln!("   Supported tools: shell, http, playbook, duckdb, auth, sink");
                eprintln!("   For other tools (postgres, python, iterator, etc.), use distributed execution");
                Ok(None)
            }
        }
    }


    /// Execute a Rhai script with access to HTTP, sleep, and utility functions

    // R-1.1 PR-2c-3: the rhai_to_json_string / json_to_rhai forwarders
    // that lived here were used only by execute_rhai_script (now
    // deleted) and rhai_http_request (now deleted), so they came out
    // with that change.  noetl-executor::template::{json_to_rhai,
    // rhai_to_json_string} remain available for any future caller
    // that needs them.

    // R-1.1 PR-2c-5: `execute_http_request` (curl subprocess) and
    // `get_auth_token` (`gcloud auth print-access-token` shellout)
    // were replaced with a call into
    // `noetl_executor::tools_bridge::dispatch_via_registry` which
    // routes through `noetl_tools::HttpTool` (reqwest) and
    // `noetl_tools::auth::GcpAuth` (gcp_auth crate) respectively.
    // See the Tool::Http arm of `execute_tool` above for the wiring.

    // R-1.1 PR-2c-6: `execute_duckdb_query` was replaced with a
    // call into `noetl_executor::tools_bridge::dispatch_via_registry`
    // which routes through `noetl_tools::tools::DuckdbTool`.  The
    // bridge's `reshape_duckdb_result` preserves the CLI's
    // SELECT-rows-array / `{"status": "ok"}` envelope shape.  Path
    // resolution + `mkdir -p` stay at the CLI call site because the
    // bridge has no knowledge of the playbook directory.  See the
    // Tool::DuckDb arm of `execute_tool` above for the wiring.

    /// Resolve DuckDB path relative to playbook or as absolute
    fn resolve_duckdb_path(&self, db_path: &str) -> Result<PathBuf> {
        if db_path.starts_with('/') || db_path.starts_with('~') {
            // Absolute path
            let expanded = shellexpand::tilde(db_path);
            Ok(PathBuf::from(expanded.as_ref()))
        } else {
            // Relative to playbook directory
            let base_dir = self
                .playbook_path
                .parent()
                .context("Failed to get playbook directory")?;
            Ok(base_dir.join(db_path))
        }
    }

    /// Resolve sink file path relative to playbook or as absolute
    fn resolve_sink_path(&self, file_path: &str) -> Result<PathBuf> {
        if file_path.starts_with('/') || file_path.starts_with('~') {
            let expanded = shellexpand::tilde(file_path);
            Ok(PathBuf::from(expanded.as_ref()))
        } else {
            let base_dir = self
                .playbook_path
                .parent()
                .context("Failed to get playbook directory")?;
            Ok(base_dir.join(file_path))
        }
    }

    /// Convert JSON array to CSV format
    fn json_to_csv(&self, json_str: &str) -> Result<String> {
        let value: serde_json::Value =
            serde_json::from_str(json_str).unwrap_or(serde_json::Value::String(json_str.to_string()));

        match value {
            serde_json::Value::Array(arr) if !arr.is_empty() => {
                // Get headers from first object
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

    /// Sink data to DuckDB table
    fn sink_to_duckdb(&self, db_path: &PathBuf, table: &str, json_data: &str) -> Result<()> {
        // Ensure parent directory exists
        if let Some(parent) = db_path.parent() {
            fs::create_dir_all(parent)?;
        }

        let conn = Connection::open(db_path).context("Failed to open DuckDB database")?;

        // Parse JSON data
        let value: serde_json::Value = serde_json::from_str(json_data)?;

        // Use DuckDB's JSON extension to insert data
        let json_escaped = json_data.replace('\'', "''");
        let insert_query = format!(
            "INSERT INTO {} SELECT * FROM read_json_auto('{}', format='array')",
            table, json_escaped
        );

        // If that fails, try a simpler approach for single objects
        match conn.execute(&insert_query, params![]) {
            Ok(_) => Ok(()),
            Err(_) => {
                // Try inserting as a single JSON object
                if let serde_json::Value::Object(obj) = &value {
                    let columns: Vec<&String> = obj.keys().collect();
                    let values: Vec<String> = obj
                        .values()
                        .map(|v| match v {
                            serde_json::Value::String(s) => format!("'{}'", s.replace('\'', "''")),
                            serde_json::Value::Null => "NULL".to_string(),
                            _ => v.to_string(),
                        })
                        .collect();

                    let query = format!(
                        "INSERT INTO {} ({}) VALUES ({})",
                        table,
                        columns.iter().map(|c| c.as_str()).collect::<Vec<_>>().join(", "),
                        values.join(", ")
                    );
                    conn.execute(&query, params![])?;
                }
                Ok(())
            }
        }
    }

    /// Sink data to GCS using gsutil
    fn sink_to_gcs(&self, gcs_uri: &str, data: &str) -> Result<()> {
        // Write to temp file first
        let temp_file = tempfile::NamedTempFile::new()?;
        fs::write(temp_file.path(), data)?;

        // Use gsutil to copy
        let output = Command::new("gsutil")
            .args(["cp", temp_file.path().to_str().unwrap(), gcs_uri])
            .output()
            .context("Failed to upload to GCS (gsutil not available?)")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            anyhow::bail!("Failed to upload to GCS: {}", stderr);
        }

        Ok(())
    }

    fn render_template(&self, template: &str, context: &ExecutionContext) -> Result<String> {
        // R-1.1 PR-2b: body extracted to noetl_executor::template.
        noetl_executor::template::render_template(template, &context.variables, &context.step_results)
    }

    /// Render template with access to JSON result via result.path notation
    fn render_template_with_result(
        &self,
        template: &str,
        context: &ExecutionContext,
        result_json: Option<&serde_json::Value>,
    ) -> Result<String> {
        // R-1.1 PR-2b: body extracted to noetl_executor::template.
        noetl_executor::template::render_template_with_result(
            template,
            &context.variables,
            &context.step_results,
            result_json,
        )
    }

    /// Get a value from JSON using a path like "status", "body.name", "items[0].id"
    fn resolve_playbook_path(&self, relative_path: &str) -> Result<PathBuf> {
        let base_dir = self
            .playbook_path
            .parent()
            .context("Failed to get playbook directory")?;
        Ok(base_dir.join(relative_path))
    }
}

struct ExecutionContext {
    variables: HashMap<String, String>,
    step_results: HashMap<String, String>,
    /// Insertion-ordered list of step names that have been executed.
    /// `step_results` is a HashMap (no stable order); this Vec gives
    /// the runner outcome a deterministic "what ran, in what order"
    /// for the JSON envelope's `executed_steps` field.
    executed_steps: Vec<String>,
}

impl ExecutionContext {
    fn new() -> Self {
        Self {
            variables: HashMap::new(),
            step_results: HashMap::new(),
            executed_steps: Vec::new(),
        }
    }

    fn set_variable(&mut self, key: String, value: String) {
        self.variables.insert(key, value);
    }

    fn set_step_result(&mut self, step_name: String, result: String) {
        // Track the first-time execution of this step. set_step_result
        // is called per-step at most once in normal flow; if it gets
        // re-called (re-entry, fan-in fan-out edge case) we keep the
        // first occurrence for the outcome's executed_steps order.
        if !self.executed_steps.iter().any(|s| s == &step_name) {
            self.executed_steps.push(step_name.clone());
        }
        self.step_results.insert(step_name.clone(), result.clone());
        // Also set as variable for easy access
        self.variables.insert(format!("{}.result", step_name), result);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_template_rendering() {
        let mut context = ExecutionContext::new();
        context.set_variable("workload.cluster".to_string(), "noetl".to_string());

        let runner = PlaybookRunner::new(PathBuf::from("test.yaml"));
        let result = runner
            .render_template(
                "kind load docker-image noetl:latest --name {{ workload.cluster }}",
                &context,
            )
            .unwrap();

        assert_eq!(result, "kind load docker-image noetl:latest --name noetl");
    }

    #[test]
    fn test_condition_evaluation_equality() {
        let context = ExecutionContext::new();
        let runner = PlaybookRunner::new(PathBuf::from("test.yaml"));

        // Test equality
        assert!(runner.evaluate_condition("'test' == 'test'", &context).unwrap());
        assert!(!runner.evaluate_condition("'test' == 'other'", &context).unwrap());
    }

    #[test]
    fn test_condition_evaluation_inequality() {
        let context = ExecutionContext::new();
        let runner = PlaybookRunner::new(PathBuf::from("test.yaml"));

        // Test inequality
        assert!(runner.evaluate_condition("'test' != 'other'", &context).unwrap());
        assert!(!runner.evaluate_condition("'test' != 'test'", &context).unwrap());
    }

    #[test]
    fn test_condition_evaluation_with_variables() {
        let mut context = ExecutionContext::new();
        context.set_variable("workload.action".to_string(), "build".to_string());

        let runner = PlaybookRunner::new(PathBuf::from("test.yaml"));

        // Test condition with variable substitution
        assert!(runner
            .evaluate_condition("{{ workload.action == 'build' }}", &context)
            .unwrap());
        assert!(!runner
            .evaluate_condition("{{ workload.action == 'deploy' }}", &context)
            .unwrap());
    }

    #[test]
    fn test_condition_evaluation_truthy() {
        let context = ExecutionContext::new();
        let runner = PlaybookRunner::new(PathBuf::from("test.yaml"));

        // Test truthy values
        assert!(runner.evaluate_condition("true", &context).unwrap());
        assert!(runner.evaluate_condition("1", &context).unwrap());
        assert!(runner.evaluate_condition("non-empty", &context).unwrap());

        // Test falsy values
        assert!(!runner.evaluate_condition("false", &context).unwrap());
        assert!(!runner.evaluate_condition("0", &context).unwrap());
        assert!(!runner.evaluate_condition("", &context).unwrap());
    }

    #[test]
    fn test_next_mode_default() {
        // NextMode should default to Exclusive
        let mode = NextMode::default();
        assert!(matches!(mode, NextMode::Exclusive));
    }

    #[test]
    fn test_executor_spec_parsing() {
        let yaml = r#"
            entry_step: "custom_start"
            final_step: "cleanup"
            no_next_is_error: true
        "#;

        let spec: ExecutorSpec = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(spec.entry_step, Some("custom_start".to_string()));
        assert_eq!(spec.final_step, Some("cleanup".to_string()));
        assert_eq!(spec.no_next_is_error, Some(true));
    }

    #[test]
    fn test_step_spec_parsing() {
        let yaml = r#"
            next_mode: inclusive
        "#;

        let spec: StepSpec = serde_yaml::from_str(yaml).unwrap();
        assert!(matches!(spec.next_mode, Some(NextMode::Inclusive)));
    }

    #[test]
    fn test_canonical_next_step_parsing() {
        let yaml = r#"
            step: process_data
            when: "{{ workload.enabled }}"
        "#;

        let next: NextStep = serde_yaml::from_str(yaml).unwrap();
        match next {
            NextStep::Canonical {
                step, when_condition, ..
            } => {
                assert_eq!(step, "process_data");
                assert_eq!(when_condition, Some("{{ workload.enabled }}".to_string()));
            }
            _ => panic!("Expected Canonical variant"),
        }
    }

    #[test]
    fn test_canonical_next_step_with_args() {
        let yaml = r#"
            step: transform
            when: "{{ vars.ready }}"
            args:
              source: input.json
              target: output.json
        "#;

        let next: NextStep = serde_yaml::from_str(yaml).unwrap();
        match next {
            NextStep::Canonical {
                step,
                when_condition,
                args,
            } => {
                assert_eq!(step, "transform");
                assert_eq!(when_condition, Some("{{ vars.ready }}".to_string()));
                assert!(args.is_some());
                let args = args.unwrap();
                assert!(args.contains_key("source"));
                assert!(args.contains_key("target"));
            }
            _ => panic!("Expected Canonical variant"),
        }
    }

    #[test]
    fn test_step_with_when_guard_parsing() {
        let yaml = r#"
            step: conditional_step
            when: "{{ workload.enabled == 'true' }}"
            desc: A step that only runs when enabled
            tool:
              kind: shell
              cmds:
                - echo "running"
        "#;

        let step: Step = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(step.step, "conditional_step");
        assert_eq!(step.when_guard, Some("{{ workload.enabled == 'true' }}".to_string()));
        assert!(step.desc.is_some());
    }

    #[test]
    fn test_playbook_entry_step_resolution() {
        let yaml = r#"
            apiVersion: noetl.io/v2
            kind: Playbook
            metadata:
              name: test_entry
            workflow:
              - step: first_step
                desc: First step in workflow
                tool:
                  kind: shell
                  cmds:
                    - echo "first"
              - step: second_step
                desc: Second step
                tool:
                  kind: shell
                  cmds:
                    - echo "second"
        "#;

        let playbook: Playbook = serde_yaml::from_str(yaml).unwrap();

        // Default entry should be workflow[0]
        let entry = playbook.workflow.first().map(|s| s.step.clone());
        assert_eq!(entry, Some("first_step".to_string()));
    }

    #[test]
    fn test_playbook_with_executor_entry_step() {
        let yaml = r#"
            apiVersion: noetl.io/v2
            kind: Playbook
            metadata:
              name: test_entry_override
            executor:
              profile: local
              spec:
                entry_step: custom_entry
            workflow:
              - step: first_step
                tool:
                  kind: shell
                  cmds:
                    - echo "first"
              - step: custom_entry
                tool:
                  kind: shell
                  cmds:
                    - echo "custom entry"
        "#;

        let playbook: Playbook = serde_yaml::from_str(yaml).unwrap();

        // Entry should be from executor.spec.entry_step
        let entry = playbook
            .executor
            .as_ref()
            .and_then(|e| e.spec.as_ref())
            .and_then(|s| s.entry_step.clone());
        assert_eq!(entry, Some("custom_entry".to_string()));
    }
}

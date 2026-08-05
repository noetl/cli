//! Condition evaluation — simple template-style equality / contains
//! / truthy checks (`evaluate_condition`) and full Rhai expression
//! evaluation (`evaluate_rhai_condition`).
//!
//! Extracted from `repos/cli/src/playbook_runner.rs` lines 771-911
//! in R-1.1 PR-2b per § H.10.3 of Appendix H of the global hybrid
//! cloud blueprint.  Both the CLI's tree walker and the worker's
//! NATS-mode runner evaluate `when` / `case` conditions the same
//! way; this module is the shared implementation.

use anyhow::Result;
use rhai::{Dynamic, Engine, Map, Scope};
use std::collections::HashMap;

/// Evaluate a simple template-style condition.  Supports:
///
/// - `{{ var == "value" }}`
/// - `{{ var != "value" }}`
/// - `{{ 'value' in var }}`
/// - `{{ var }}` (truthy check)
///
/// Variable references are substituted from the supplied `variables`
/// map before the comparison is run.
pub fn evaluate_condition(
    condition: &str,
    variables: &HashMap<String, String>,
) -> Result<bool> {
    // Extract content from {{ ... }} if present.
    let expression = if condition.trim().starts_with("{{") && condition.trim().ends_with("}}") {
        condition
            .trim()
            .strip_prefix("{{")
            .unwrap()
            .strip_suffix("}}")
            .unwrap()
            .trim()
    } else {
        condition.trim()
    };

    // Replace variables within the expression.
    //
    // noetl/cli#76 — this loop used to iterate `variables` directly.  That is a
    // `HashMap`, so the iteration order is randomised per instance, and each
    // step is a naive substring `replace`.  When one variable NAME is a prefix
    // of another the result depends on which happened to come first:
    //
    //   vars: status="ok", status_code="200"   expression: status_code == "200"
    //     status_code first ->  200 == "200"      -> TRUE
    //     status      first ->  ok_code == "200"  -> FALSE
    //
    // Measured over 2000 constructions of that exact map: 858 true / 1142
    // false.  That is the whole of cli#76's "a true `when:` arc takes its
    // branch only about half the time" — the arc order is deterministic (a
    // `Vec`, first match wins); the CONDITION was not.
    //
    // Fixed by making the order total and prefix-safe: longest key first, ties
    // broken lexicographically.  A longer name is therefore always substituted
    // before any name that is a prefix of it, and the same inputs now always
    // produce the same expression.
    let mut keys: Vec<&String> = variables.keys().collect();
    keys.sort_by(|a, b| b.len().cmp(&a.len()).then_with(|| a.as_str().cmp(b.as_str())));
    let mut rendered = expression.to_string();
    for key in keys {
        rendered = rendered.replace(key, &variables[key]);
    }

    fn strip_quotes(s: &str) -> String {
        let s = s.trim();
        if (s.starts_with('"') && s.ends_with('"')) || (s.starts_with('\'') && s.ends_with('\'')) {
            s[1..s.len() - 1].to_string()
        } else {
            s.to_string()
        }
    }

    if rendered.contains("==") {
        let parts: Vec<&str> = rendered.split("==").map(|s| s.trim()).collect();
        if parts.len() == 2 {
            return Ok(strip_quotes(parts[0]) == strip_quotes(parts[1]));
        }
    }

    if rendered.contains("!=") {
        let parts: Vec<&str> = rendered.split("!=").map(|s| s.trim()).collect();
        if parts.len() == 2 {
            return Ok(strip_quotes(parts[0]) != strip_quotes(parts[1]));
        }
    }

    // 'in' operator (e.g., "'value' in var" or "var in list").
    if rendered.contains(" in ") {
        let parts: Vec<&str> = rendered.split(" in ").map(|s| s.trim()).collect();
        if parts.len() == 2 {
            let needle = strip_quotes(parts[0]);
            let haystack = strip_quotes(parts[1]);
            return Ok(haystack.contains(&needle));
        }
    }

    // Truthy check.
    //
    // noetl/ai-meta#231 — this used to be a CASE-SENSITIVE `value != "false"`.
    // The template engine renders a boolean Python-style, so a `when:` guard on
    // a false boolean rendered `"False"`, which is neither empty nor the
    // lowercase literal, and the arc fired anyway.  Deterministically wrong in
    // one direction, which is the other half of noetl/cli#76: the substitution
    // ordering above made a true arc fire about half the time, and this made a
    // FALSE arc fire every time.
    //
    // The falsy set is also aligned with the server's
    // `orchestrate-core::TemplateRenderer::evaluate_condition`, which already
    // lower-cases and already treats these as falsy.  This module's own doc
    // comment claims the CLI tree walker and the worker runner "evaluate
    // `when` / `case` conditions the same way" — they did not evaluate them the
    // same way as the server, so one playbook could take different branches in
    // `--runtime local` and on a cluster.  That divergence is a defect in its
    // own right.
    let value = strip_quotes(&rendered).trim().to_lowercase();
    Ok(!matches!(
        value.as_str(),
        "" | "false" | "0" | "no" | "none" | "null" | "off" | "{}" | "[]"
    ))
}

/// Evaluate a Rhai expression that returns a boolean condition.
///
/// The scope is populated with `workload.*`, `vars.*`, and
/// `<step>.<field>` maps derived from the supplied `variables` map.
/// Three helper functions are registered: `eq(a, b)`, `ne(a, b)`,
/// `contains(haystack, needle)`.
pub fn evaluate_rhai_condition(
    code: &str,
    variables: &HashMap<String, String>,
) -> Result<bool> {
    let mut engine = Engine::new();
    let mut scope = Scope::new();

    // workload.* -> scope.workload map.
    let mut workload_map = Map::new();
    for (key, value) in variables {
        if key.starts_with("workload.") {
            let short_key = key.strip_prefix("workload.").unwrap_or(key);
            workload_map.insert(short_key.to_string().into(), Dynamic::from(value.clone()));
        }
    }
    scope.push("workload", workload_map);

    // vars.* -> scope.vars map.
    let mut vars_map = Map::new();
    for (key, value) in variables {
        if key.starts_with("vars.") {
            let short_key = key.strip_prefix("vars.").unwrap_or(key);
            vars_map.insert(short_key.to_string().into(), Dynamic::from(value.clone()));
        }
    }
    scope.push("vars", vars_map);

    // <step>.<field> -> scope.<step> map.
    for (key, value) in variables {
        if !key.starts_with("workload.") && !key.starts_with("vars.") && key.contains('.') {
            let parts: Vec<&str> = key.splitn(2, '.').collect();
            if parts.len() == 2 {
                let step_name = parts[0];
                let field_name = parts[1];

                if !scope.contains(step_name) {
                    scope.push(step_name.to_string(), Map::new());
                }

                if let Some(step_map) = scope.get_mut(step_name) {
                    if let Some(map) = step_map.clone().try_cast::<Map>() {
                        let mut map = map;
                        map.insert(field_name.to_string().into(), Dynamic::from(value.clone()));
                        *step_map = Dynamic::from(map);
                    }
                }
            }
        }
    }

    // Comparison helpers.
    engine.register_fn("eq", |a: &str, b: &str| -> bool { a == b });
    engine.register_fn("ne", |a: &str, b: &str| -> bool { a != b });
    engine.register_fn("contains", |haystack: &str, needle: &str| -> bool {
        haystack.contains(needle)
    });

    let result = engine
        .eval_with_scope::<Dynamic>(&mut scope, code)
        .map_err(|e| anyhow::anyhow!("Rhai condition error: {}", e))?;

    if result.is_bool() {
        Ok(result.as_bool().unwrap_or(false))
    } else if result.is_int() {
        Ok(result.as_int().unwrap_or(0) != 0)
    } else if result.is_string() {
        let s = result.into_string().unwrap_or_default();
        Ok(!s.is_empty() && s != "false" && s != "0")
    } else {
        Ok(!result.is_unit())
    }
}

// ===========================================================================
// R-1.2 PR-2b — structured condition surface
//
// The CLI's `evaluate_condition` / `evaluate_rhai_condition` above work
// on template-style **strings** with a flat `HashMap<String, String>`
// variable map.  That matches how the CLI's tree walker calls into the
// YAML's `when:` / `if:` blocks.
//
// The worker (R-1.2 PR-2c/d) receives commands from NATS that carry
// **structured JSON** `case` / `when` blocks: each condition is a
// `{ left, op, right }` triple, and the worker evaluates them against
// `noetl_tools::context::ExecutionContext`.  The worker's pre-PR-2b
// inline implementation lived in `repos/worker/src/executor/case_evaluator.rs`
// (~437 LoC).  This module exposes the condition primitive so both
// binaries agree on operator semantics.
//
// The wrapper struct + Case/CaseAction control-flow types stay in the
// worker — they're tied to the worker's pull-loop dispatch semantics
// per § H.10.
// ===========================================================================

use noetl_tools::context::ExecutionContext as ToolsExecutionContext;
use noetl_tools::template::TemplateEngine;
use serde::{Deserialize, Serialize};

/// Operator for [`evaluate_structured_condition`].
///
/// Twelve variants matching the worker's pre-PR-2b inline operator
/// set.  Wire format: lowercase snake-case (`"eq"`, `"not_in"`, etc.).
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Operator {
    /// Equality (`left == right`).
    #[default]
    Eq,
    /// Inequality (`left != right`).
    Ne,
    /// Greater than (numeric).
    Gt,
    /// Less than (numeric).
    Lt,
    /// Greater than or equal (numeric).
    Gte,
    /// Less than or equal (numeric).
    Lte,
    /// `left` (string) contains `right` (string).
    Contains,
    /// `left` (string) matches `right` (regex).
    Matches,
    /// `left` is truthy (right ignored).
    Truthy,
    /// `left` is falsy (right ignored).
    Falsy,
    /// `left` is an element of `right` (array).
    In,
    /// `left` is NOT an element of `right` (array).
    NotIn,
}

/// Structured condition the worker carries on its NATS command
/// envelopes.  Lifted from the worker's pre-PR-2b
/// `executor::case_evaluator::Condition`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Condition {
    /// Left-hand side value or variable reference.  Resolved against
    /// the context (variable lookup), against `result.<path>`
    /// (JSON-path navigation of the tool result), or as a literal
    /// string after template substitution.
    pub left: String,

    /// Operator.
    #[serde(default)]
    pub op: Operator,

    /// Right-hand side value.  May contain templates;
    /// [`evaluate_structured_condition`] renders them against the
    /// supplied context before applying the operator.
    #[serde(default)]
    pub right: Option<serde_json::Value>,
}

/// Evaluate a structured condition against `ctx` + optional tool
/// `result`.
///
/// Behaviour matches the worker's pre-PR-2b
/// `CaseEvaluator::evaluate_condition`:
///
/// - `condition.left` resolution order:
///   1. `"result"` → the supplied tool result (or `Null` if `None`).
///   2. `"result.<path>"` → JSON path navigation of the result.
///   3. Variable lookup via `ctx.get_variable(&left)`.
///   4. Template rendering via `TemplateEngine::render`.
///   5. Literal string.
/// - `condition.right` is template-rendered against `ctx` before use.
/// - Operator semantics match the worker's inline implementation.
///
/// This function is pure and synchronous — no I/O, no async.
pub fn evaluate_structured_condition(
    condition: &Condition,
    ctx: &ToolsExecutionContext,
    result: Option<&serde_json::Value>,
) -> Result<bool> {
    let template_engine = TemplateEngine::new();
    let left = resolve_value(&condition.left, ctx, result, &template_engine)?;
    let right = condition
        .right
        .as_ref()
        .map(|r| resolve_json_value(r, ctx, &template_engine))
        .transpose()?;

    match condition.op {
        Operator::Eq => Ok(left == right.unwrap_or(serde_json::Value::Null)),
        Operator::Ne => Ok(left != right.unwrap_or(serde_json::Value::Null)),
        Operator::Gt => compare_numeric(&left, &right, |a, b| a > b),
        Operator::Lt => compare_numeric(&left, &right, |a, b| a < b),
        Operator::Gte => compare_numeric(&left, &right, |a, b| a >= b),
        Operator::Lte => compare_numeric(&left, &right, |a, b| a <= b),
        Operator::Contains => {
            let left_str = left.as_str().unwrap_or("");
            let right_str = right.as_ref().and_then(|r| r.as_str()).unwrap_or("");
            Ok(left_str.contains(right_str))
        }
        Operator::Matches => {
            let left_str = left.as_str().unwrap_or("");
            let pattern = right.as_ref().and_then(|r| r.as_str()).unwrap_or("");
            let re = regex::Regex::new(pattern)
                .map_err(|e| anyhow::anyhow!("Invalid regex: {}", e))?;
            Ok(re.is_match(left_str))
        }
        Operator::Truthy => Ok(is_truthy(&left)),
        Operator::Falsy => Ok(!is_truthy(&left)),
        Operator::In => {
            if let Some(serde_json::Value::Array(arr)) = &right {
                Ok(arr.contains(&left))
            } else {
                Ok(false)
            }
        }
        Operator::NotIn => {
            if let Some(serde_json::Value::Array(arr)) = &right {
                Ok(!arr.contains(&left))
            } else {
                Ok(true)
            }
        }
    }
}

/// Resolve a value reference to a JSON value.  See
/// [`evaluate_structured_condition`] for the resolution order.
fn resolve_value(
    value: &str,
    ctx: &ToolsExecutionContext,
    result: Option<&serde_json::Value>,
    template_engine: &TemplateEngine,
) -> Result<serde_json::Value> {
    if let Some(path) = value.strip_prefix("result.") {
        if let Some(res) = result {
            return Ok(json_path(res, path)
                .cloned()
                .unwrap_or(serde_json::Value::Null));
        }
        return Ok(serde_json::Value::Null);
    }

    if value == "result" {
        return Ok(result.cloned().unwrap_or(serde_json::Value::Null));
    }

    if let Some(var) = ctx.get_variable(value) {
        return Ok(var.clone());
    }

    if TemplateEngine::is_template(value) {
        let template_ctx = ctx.to_template_context();
        let rendered = template_engine
            .render(value, &template_ctx)
            .map_err(|e| anyhow::anyhow!(e))?;
        return Ok(serde_json::from_str(&rendered).unwrap_or(serde_json::json!(rendered)));
    }

    Ok(serde_json::json!(value))
}

/// Resolve a JSON value that might contain templates.
fn resolve_json_value(
    value: &serde_json::Value,
    ctx: &ToolsExecutionContext,
    template_engine: &TemplateEngine,
) -> Result<serde_json::Value> {
    let template_ctx = ctx.to_template_context();
    template_engine
        .render_value(value, &template_ctx)
        .map_err(|e| anyhow::anyhow!(e))
}

/// Navigate a JSON path (dot-delimited keys + optional numeric
/// indices for arrays).
fn json_path<'a>(value: &'a serde_json::Value, path: &str) -> Option<&'a serde_json::Value> {
    let mut current = value;
    for segment in path.split('.') {
        match current {
            serde_json::Value::Object(obj) => {
                current = obj.get(segment)?;
            }
            serde_json::Value::Array(arr) => {
                let idx: usize = segment.parse().ok()?;
                current = arr.get(idx)?;
            }
            _ => return None,
        }
    }
    Some(current)
}

/// Compare two JSON values numerically.
fn compare_numeric<F>(
    left: &serde_json::Value,
    right: &Option<serde_json::Value>,
    cmp: F,
) -> Result<bool>
where
    F: Fn(f64, f64) -> bool,
{
    let left_num = value_to_f64(left)?;
    let right_num = value_to_f64(right.as_ref().unwrap_or(&serde_json::Value::Null))?;
    Ok(cmp(left_num, right_num))
}

/// Check if a JSON value is truthy (empty / zero / false → falsy).
fn is_truthy(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Null => false,
        serde_json::Value::Bool(b) => *b,
        serde_json::Value::Number(n) => n.as_f64().map(|f| f != 0.0).unwrap_or(false),
        serde_json::Value::String(s) => !s.is_empty(),
        serde_json::Value::Array(a) => !a.is_empty(),
        serde_json::Value::Object(o) => !o.is_empty(),
    }
}

/// Convert a JSON value to f64.  Booleans become 0.0 / 1.0; nulls
/// become 0.0; strings parse via `FromStr`.
fn value_to_f64(value: &serde_json::Value) -> Result<f64> {
    match value {
        serde_json::Value::Number(n) => n
            .as_f64()
            .ok_or_else(|| anyhow::anyhow!("Invalid number")),
        serde_json::Value::String(s) => s
            .parse()
            .map_err(|_| anyhow::anyhow!("Cannot parse '{s}' as number")),
        serde_json::Value::Bool(b) => Ok(if *b { 1.0 } else { 0.0 }),
        serde_json::Value::Null => Ok(0.0),
        _ => Err(anyhow::anyhow!("Cannot convert {value:?} to number")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// noetl/cli#76 — a true `when:` arc fired only about half the time.
    ///
    /// The arc order was never the problem (arcs are a `Vec`, first match
    /// wins). The CONDITION was: variable substitution iterated a `HashMap`,
    /// whose order is randomised per instance, and each step was a naive
    /// substring `replace`. With `status` and `status_code` in scope:
    ///
    ///   status_code first ->  `200 == "200"`      -> true
    ///   status      first ->  `ok_code == "200"`  -> false
    ///
    /// Measured 858/1142 over 2000 constructions before the fix.
    ///
    /// Run enough times that the pre-fix code cannot pass by luck: at ~43%
    /// per-iteration failure, 200 iterations leave a false-pass probability
    /// around 0.57^200, which is nil.
    #[test]
    fn overlapping_variable_names_evaluate_deterministically() {
        for i in 0..200 {
            let mut vars = HashMap::new();
            vars.insert("status".to_string(), "ok".to_string());
            vars.insert("status_code".to_string(), "200".to_string());
            assert!(
                evaluate_condition("{{ status_code == \"200\" }}", &vars).unwrap(),
                "iteration {i}: the longer name must always win, or the arc fires at random"
            );
        }
    }

    /// noetl/ai-meta#231 — a boolean renders Python-style, so a false guard
    /// arrived as `"False"`.  The old check was case-sensitive against the
    /// lowercase literal, so the arc fired anyway: deterministically wrong,
    /// every time, in the direction that silently runs work it should not.
    #[test]
    fn a_python_style_false_is_falsy() {
        let mut vars = HashMap::new();
        vars.insert("flag".to_string(), "False".to_string());
        assert!(
            !evaluate_condition("{{ flag }}", &vars).unwrap(),
            "a rendered `False` must be falsy — this fired the arc before #231"
        );
        vars.insert("flag".to_string(), "True".to_string());
        assert!(evaluate_condition("{{ flag }}", &vars).unwrap());
    }

    /// Aligned with the server's falsy set, so one playbook cannot take a
    /// different branch under `--runtime local` than on a cluster.
    #[test]
    fn the_falsy_set_matches_the_server() {
        for falsy in ["", "false", "False", "FALSE", "0", "no", "none", "None", "null", "off", "{}", "[]"] {
            let mut vars = HashMap::new();
            vars.insert("v".to_string(), falsy.to_string());
            assert!(
                !evaluate_condition("{{ v }}", &vars).unwrap(),
                "{falsy:?} must be falsy, as it is in orchestrate-core"
            );
        }
        for truthy in ["true", "True", "1", "yes", "ok", "req-123"] {
            let mut vars = HashMap::new();
            vars.insert("v".to_string(), truthy.to_string());
            assert!(
                evaluate_condition("{{ v }}", &vars).unwrap(),
                "{truthy:?} must be truthy"
            );
        }
    }

    /// The prefix must still resolve on its own — the fix orders the
    /// substitution, it does not drop the shorter key.
    #[test]
    fn the_shorter_name_still_substitutes() {
        let mut vars = HashMap::new();
        vars.insert("status".to_string(), "ok".to_string());
        vars.insert("status_code".to_string(), "200".to_string());
        for _ in 0..50 {
            assert!(evaluate_condition("{{ status == \"ok\" }}", &vars).unwrap());
        }
    }

    /// Three-way overlap, so the ordering is a total order and not a lucky
    /// pairwise swap.
    #[test]
    fn a_three_way_overlap_is_stable() {
        let mut vars = HashMap::new();
        vars.insert("a".to_string(), "1".to_string());
        vars.insert("ab".to_string(), "2".to_string());
        vars.insert("abc".to_string(), "3".to_string());
        for _ in 0..200 {
            assert!(evaluate_condition("{{ abc == \"3\" }}", &vars).unwrap());
            assert!(evaluate_condition("{{ ab == \"2\" }}", &vars).unwrap());
            assert!(evaluate_condition("{{ a == \"1\" }}", &vars).unwrap());
        }
    }

    fn vars(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    #[test]
    fn evaluate_condition_equality() {
        let v = HashMap::new();
        assert!(evaluate_condition("'test' == 'test'", &v).unwrap());
        assert!(!evaluate_condition("'test' == 'other'", &v).unwrap());
    }

    #[test]
    fn evaluate_condition_inequality() {
        let v = HashMap::new();
        assert!(evaluate_condition("'test' != 'other'", &v).unwrap());
        assert!(!evaluate_condition("'test' != 'test'", &v).unwrap());
    }

    #[test]
    fn evaluate_condition_in_operator() {
        let v = HashMap::new();
        assert!(evaluate_condition("'foo' in 'foobar'", &v).unwrap());
        assert!(!evaluate_condition("'baz' in 'foobar'", &v).unwrap());
    }

    #[test]
    fn evaluate_condition_substitutes_variables() {
        let v = vars(&[("workload.action", "deploy")]);
        assert!(evaluate_condition("workload.action == 'deploy'", &v).unwrap());
        assert!(!evaluate_condition("workload.action == 'undeploy'", &v).unwrap());
    }

    #[test]
    fn evaluate_rhai_condition_workload_field() {
        let v = vars(&[("workload.count", "5")]);
        assert!(evaluate_rhai_condition("workload.count == \"5\"", &v).unwrap());
        assert!(!evaluate_rhai_condition("workload.count == \"6\"", &v).unwrap());
    }

    #[test]
    fn evaluate_rhai_condition_helpers() {
        let v = vars(&[("workload.action", "DEPLOY")]);
        assert!(evaluate_rhai_condition("eq(workload.action, \"DEPLOY\")", &v).unwrap());
        assert!(evaluate_rhai_condition(
            "contains(workload.action, \"DEP\")",
            &v
        )
        .unwrap());
    }

    // ---- R-1.2 PR-2b — structured condition tests --------------------

    fn tools_ctx_with(pairs: &[(&str, serde_json::Value)]) -> ToolsExecutionContext {
        let mut ctx = ToolsExecutionContext::default();
        for (k, v) in pairs {
            ctx.set_variable(*k, v.clone());
        }
        ctx
    }

    #[test]
    fn structured_eq_against_variable() {
        let ctx = tools_ctx_with(&[("status", serde_json::json!("success"))]);
        let cond = Condition {
            left: "status".into(),
            op: Operator::Eq,
            right: Some(serde_json::json!("success")),
        };
        assert!(evaluate_structured_condition(&cond, &ctx, None).unwrap());
        let cond_fail = Condition {
            left: "status".into(),
            op: Operator::Eq,
            right: Some(serde_json::json!("failed")),
        };
        assert!(!evaluate_structured_condition(&cond_fail, &ctx, None).unwrap());
    }

    #[test]
    fn structured_ne_inverts_eq() {
        let ctx = tools_ctx_with(&[("status", serde_json::json!("ok"))]);
        let cond = Condition {
            left: "status".into(),
            op: Operator::Ne,
            right: Some(serde_json::json!("error")),
        };
        assert!(evaluate_structured_condition(&cond, &ctx, None).unwrap());
    }

    #[test]
    fn structured_numeric_comparisons() {
        let ctx = tools_ctx_with(&[("count", serde_json::json!(10))]);
        for (op, rhs, expected) in [
            (Operator::Gt, 5, true),
            (Operator::Gt, 10, false),
            (Operator::Gte, 10, true),
            (Operator::Lt, 100, true),
            (Operator::Lte, 10, true),
        ] {
            let cond = Condition {
                left: "count".into(),
                op,
                right: Some(serde_json::json!(rhs)),
            };
            assert_eq!(
                evaluate_structured_condition(&cond, &ctx, None).unwrap(),
                expected,
                "op {:?} vs {} expected {}",
                cond.op,
                rhs,
                expected
            );
        }
    }

    #[test]
    fn structured_contains_matches_strings() {
        let ctx = tools_ctx_with(&[("msg", serde_json::json!("hello world"))]);
        let cond = Condition {
            left: "msg".into(),
            op: Operator::Contains,
            right: Some(serde_json::json!("world")),
        };
        assert!(evaluate_structured_condition(&cond, &ctx, None).unwrap());
        let cond_no = Condition {
            left: "msg".into(),
            op: Operator::Contains,
            right: Some(serde_json::json!("zzz")),
        };
        assert!(!evaluate_structured_condition(&cond_no, &ctx, None).unwrap());
    }

    #[test]
    fn structured_matches_regex() {
        let ctx = tools_ctx_with(&[("user", serde_json::json!("alice@example.com"))]);
        let cond = Condition {
            left: "user".into(),
            op: Operator::Matches,
            right: Some(serde_json::json!(r"^\w+@\w+\.com$")),
        };
        assert!(evaluate_structured_condition(&cond, &ctx, None).unwrap());
    }

    #[test]
    fn structured_truthy_falsy() {
        let ctx = tools_ctx_with(&[
            ("on", serde_json::json!(true)),
            ("zero", serde_json::json!(0)),
            ("empty", serde_json::json!("")),
            ("nonempty", serde_json::json!("x")),
        ]);
        let truthy_on = Condition {
            left: "on".into(),
            op: Operator::Truthy,
            right: None,
        };
        assert!(evaluate_structured_condition(&truthy_on, &ctx, None).unwrap());
        let falsy_zero = Condition {
            left: "zero".into(),
            op: Operator::Falsy,
            right: None,
        };
        assert!(evaluate_structured_condition(&falsy_zero, &ctx, None).unwrap());
        let falsy_empty = Condition {
            left: "empty".into(),
            op: Operator::Falsy,
            right: None,
        };
        assert!(evaluate_structured_condition(&falsy_empty, &ctx, None).unwrap());
        let truthy_x = Condition {
            left: "nonempty".into(),
            op: Operator::Truthy,
            right: None,
        };
        assert!(evaluate_structured_condition(&truthy_x, &ctx, None).unwrap());
    }

    #[test]
    fn structured_in_and_not_in() {
        let ctx = tools_ctx_with(&[("role", serde_json::json!("admin"))]);
        let in_cond = Condition {
            left: "role".into(),
            op: Operator::In,
            right: Some(serde_json::json!(["admin", "ops", "dev"])),
        };
        assert!(evaluate_structured_condition(&in_cond, &ctx, None).unwrap());
        let not_in_cond = Condition {
            left: "role".into(),
            op: Operator::NotIn,
            right: Some(serde_json::json!(["guest", "viewer"])),
        };
        assert!(evaluate_structured_condition(&not_in_cond, &ctx, None).unwrap());
    }

    #[test]
    fn structured_left_resolves_result_path() {
        let ctx = ToolsExecutionContext::default();
        let result = serde_json::json!({
            "status": "ok",
            "data": {"count": 42}
        });
        let cond = Condition {
            left: "result.data.count".into(),
            op: Operator::Eq,
            right: Some(serde_json::json!(42)),
        };
        assert!(evaluate_structured_condition(&cond, &ctx, Some(&result)).unwrap());
    }

    #[test]
    fn structured_left_resolves_bare_result() {
        let ctx = ToolsExecutionContext::default();
        let result = serde_json::json!("hello");
        let cond = Condition {
            left: "result".into(),
            op: Operator::Eq,
            right: Some(serde_json::json!("hello")),
        };
        assert!(evaluate_structured_condition(&cond, &ctx, Some(&result)).unwrap());
    }

    #[test]
    fn structured_operator_serializes_snake_case() {
        let cond = Condition {
            left: "x".into(),
            op: Operator::NotIn,
            right: None,
        };
        let s = serde_json::to_string(&cond).unwrap();
        assert!(s.contains("\"not_in\""), "got: {s}");
        let parsed: Condition = serde_json::from_str(&s).unwrap();
        assert!(matches!(parsed.op, Operator::NotIn));
    }

    #[test]
    fn structured_in_returns_false_when_right_not_array() {
        let ctx = tools_ctx_with(&[("x", serde_json::json!(1))]);
        let cond = Condition {
            left: "x".into(),
            op: Operator::In,
            right: Some(serde_json::json!("not an array")),
        };
        assert!(!evaluate_structured_condition(&cond, &ctx, None).unwrap());
    }
}

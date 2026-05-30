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
    let mut rendered = expression.to_string();
    for (key, value) in variables {
        rendered = rendered.replace(key, value);
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

    // Truthy check — not empty, not "false", not "0".
    let value = strip_quotes(&rendered);
    Ok(!value.is_empty() && value != "false" && value != "0")
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

#[cfg(test)]
mod tests {
    use super::*;

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
}

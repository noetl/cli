//! Template rendering — `{{ workload.var }}` / `{{ step.result }}` /
//! `{{ result.path }}` substitution with a minimal filter set.
//!
//! Extracted from `repos/cli/src/playbook_runner.rs` lines 1448-2006
//! in R-1.1 PR-2b per § H.10.3 of Appendix H of the global hybrid
//! cloud blueprint.  The CLI's tree walker (`playbook_runner.rs`)
//! and the worker's NATS-mode runner both render templates the same
//! way; this module is the shared implementation they call into.
//!
//! Functions take `&HashMap<String, String>` slices of the
//! per-execution variables and step results so each binary owns its
//! own context shape but feeds the same data into rendering.

use anyhow::Result;
use rhai::Dynamic;
use std::collections::HashMap;

/// Render a template by substituting `{{ var }}`, `{{ var | filter }}`,
/// `{{ workload.var }}`, `{{ vars.var }}`, and `{{ step.result }}`
/// references against the supplied variable + step-result maps.
///
/// Supported filters: `lower`, `upper`, `trim`, `default`.
///
/// Substitution is a single left-to-right scan: a substituted value is
/// never re-scanned, so `{{ ... }}` text inside a value (e.g. an email
/// body or HTTP response) cannot pull in other variables.  Unknown
/// tokens are left untouched.
pub fn render_template(
    template: &str,
    variables: &HashMap<String, String>,
    step_results: &HashMap<String, String>,
) -> Result<String> {
    Ok(render_tokens(template, variables, step_results, None))
}

/// Filter token: `{{ var | filter }}`.
const FILTER_PATTERN: &str =
    r"\{\{\s*(?P<fvar>[a-zA-Z_][a-zA-Z0-9_.]*)\s*\|\s*(?P<fname>[a-zA-Z_]+)\s*\}\}";
/// Plain token: exactly `{{ key }}` (one space each side), matching the
/// placeholders the previous `str::replace` passes looked for.
const PLAIN_PATTERN: &str = r"\{\{ (?P<key>[^{}]*?) \}\}";
/// Result token: `{{ result.path }}` with an optional filter.
const RESULT_PATTERN: &str =
    r"\{\{\s*result\.(?P<rpath>[a-zA-Z0-9_.\[\]]+)\s*(?:\|\s*(?P<rfilter>[a-zA-Z_]+(?:\([^)]*\))?))?\s*\}\}";

/// Shared single-pass renderer.  `result_json` is `Some` when called
/// from [`render_template_with_result`], which enables `{{ result.* }}`
/// tokens (taking precedence over the other token kinds, as before).
fn render_tokens(
    template: &str,
    variables: &HashMap<String, String>,
    step_results: &HashMap<String, String>,
    result_json: Option<Option<&serde_json::Value>>,
) -> String {
    let pattern = if result_json.is_some() {
        format!("{}|{}|{}", RESULT_PATTERN, FILTER_PATTERN, PLAIN_PATTERN)
    } else {
        format!("{}|{}", FILTER_PATTERN, PLAIN_PATTERN)
    };
    let token_regex = regex::Regex::new(&pattern).unwrap();

    let rendered = token_regex.replace_all(template, |caps: &regex::Captures| {
        if let Some(path) = caps.name("rpath") {
            let filter = caps.name("rfilter").map(|m| m.as_str());
            return resolve_result(result_json.flatten(), path.as_str(), filter);
        }
        if let (Some(var), Some(filter)) = (caps.name("fvar"), caps.name("fname")) {
            return resolve_filter(variables, var.as_str(), filter.as_str());
        }
        let key = &caps["key"];
        resolve_plain(variables, step_results, key)
            .map(str::to_string)
            .unwrap_or_else(|| caps[0].to_string())
    });

    rendered.trim().to_string()
}

/// Resolve a `{{ var | filter }}` token.  Missing variables render as
/// the empty string (after the `workload.<name>` fallback).
fn resolve_filter(variables: &HashMap<String, String>, var_name: &str, filter_name: &str) -> String {
    let value = variables
        .get(var_name)
        .or_else(|| variables.get(&format!("workload.{}", var_name)))
        .map(|s| s.as_str())
        .unwrap_or("");

    match filter_name {
        "lower" => value.to_lowercase(),
        "upper" => value.to_uppercase(),
        "trim" => value.trim().to_string(),
        // `default` without an argument renders the value (empty if unset).
        _ => value.to_string(),
    }
}

/// Resolve a plain `{{ key }}` token with the same precedence as the
/// previous replace passes: `workload.*`, `vars.*`, `<step>.result`,
/// then a direct key lookup.  `None` leaves the token untouched.
fn resolve_plain<'a>(
    variables: &'a HashMap<String, String>,
    step_results: &'a HashMap<String, String>,
    key: &str,
) -> Option<&'a str> {
    if key.starts_with("workload.") || key.starts_with("vars.") {
        if let Some(value) = variables.get(key) {
            return Some(value);
        }
    }
    if let Some(step_name) = key.strip_suffix(".result") {
        if let Some(value) = step_results.get(step_name) {
            return Some(value);
        }
    }
    variables.get(key).map(|s| s.as_str())
}

/// Resolve a `{{ result.path | filter }}` token against the result JSON.
fn resolve_result(result_json: Option<&serde_json::Value>, path: &str, filter: Option<&str>) -> String {
    let Some(json) = result_json else {
        return "".to_string();
    };

    // Navigate the JSON path.
    let value = get_json_path(json, path);
    let value_str = match &value {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Null => "".to_string(),
        other => other.to_string(),
    };

    // Apply the default filter if present.
    match filter {
        Some(f) if (f == "default" || f.starts_with("default(")) && (value_str.is_empty() || value_str == "null") => {
            // Extract default value from default('value') or default("").
            if let Some(start) = f.find('(') {
                let inner = &f[start + 1..f.len() - 1];
                inner.trim_matches(|c| c == '\'' || c == '"').to_string()
            } else {
                "".to_string()
            }
        }
        _ => value_str,
    }
}

/// Render a template that may reference `{{ result.path }}` against the
/// supplied JSON result value (e.g. the previous step's HTTP response
/// body) along with the regular [`render_template`] references, in one
/// pass so values from the result are never re-expanded.
pub fn render_template_with_result(
    template: &str,
    variables: &HashMap<String, String>,
    step_results: &HashMap<String, String>,
    result_json: Option<&serde_json::Value>,
) -> Result<String> {
    Ok(render_tokens(template, variables, step_results, Some(result_json)))
}

/// Get a value from JSON using a path like `"status"`, `"body.name"`,
/// or `"items[0].id"`.
pub fn get_json_path(json: &serde_json::Value, path: &str) -> serde_json::Value {
    let parts: Vec<&str> = path.split('.').collect();
    let mut current = json.clone();

    for part in parts {
        // Handle array index notation like items[0].
        if part.contains('[') {
            if let Some(bracket_pos) = part.find('[') {
                let key = &part[..bracket_pos];
                let idx_str = &part[bracket_pos + 1..part.len() - 1];

                if !key.is_empty() {
                    current = current.get(key).cloned().unwrap_or(serde_json::Value::Null);
                }

                if let Ok(idx) = idx_str.parse::<usize>() {
                    current = current.get(idx).cloned().unwrap_or(serde_json::Value::Null);
                }
            }
        } else {
            current = current.get(part).cloned().unwrap_or(serde_json::Value::Null);
        }
    }

    current
}

/// Convert a `serde_json::Value` into a `rhai::Dynamic` for scripting.
pub fn json_to_rhai(value: &serde_json::Value) -> Dynamic {
    use rhai::{Array, Map};

    match value {
        serde_json::Value::Null => Dynamic::UNIT,
        serde_json::Value::Bool(b) => Dynamic::from(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Dynamic::from(i)
            } else if let Some(f) = n.as_f64() {
                Dynamic::from(f)
            } else {
                Dynamic::from(n.to_string())
            }
        }
        serde_json::Value::String(s) => Dynamic::from(s.clone()),
        serde_json::Value::Array(arr) => {
            let mut rhai_array = Array::new();
            for item in arr {
                rhai_array.push(json_to_rhai(item));
            }
            Dynamic::from(rhai_array)
        }
        serde_json::Value::Object(obj) => {
            let mut rhai_map = Map::new();
            for (k, v) in obj {
                rhai_map.insert(k.to_string().into(), json_to_rhai(v));
            }
            Dynamic::from(rhai_map)
        }
    }
}

/// Stringify a `rhai::Dynamic` value into a JSON-shaped string for
/// embedding into rendered output.
pub fn rhai_to_json_string(value: &Dynamic) -> String {
    if value.is_unit() {
        "null".to_string()
    } else if let Some(b) = value.clone().try_cast::<bool>() {
        b.to_string()
    } else if let Some(i) = value.clone().try_cast::<i64>() {
        i.to_string()
    } else if let Some(f) = value.clone().try_cast::<f64>() {
        f.to_string()
    } else if let Some(s) = value.clone().try_cast::<String>() {
        // Quote string values.
        format!("\"{}\"", s)
    } else if let Some(arr) = value.clone().try_cast::<rhai::Array>() {
        let items: Vec<String> = arr.iter().map(rhai_to_json_string).collect();
        format!("[{}]", items.join(","))
    } else if let Some(map) = value.clone().try_cast::<rhai::Map>() {
        let pairs: Vec<String> = map
            .iter()
            .map(|(k, v)| format!("\"{}\":{}", k, rhai_to_json_string(v)))
            .collect();
        format!("{{{}}}", pairs.join(","))
    } else {
        // Fallback to debug representation.
        format!("{:?}", value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vars(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    #[test]
    fn render_template_substitutes_workload_var() {
        let variables = vars(&[("workload.cluster", "noetl")]);
        let step_results = HashMap::new();
        let result = render_template(
            "kind load docker-image noetl:latest --name {{ workload.cluster }}",
            &variables,
            &step_results,
        )
        .unwrap();
        assert_eq!(result, "kind load docker-image noetl:latest --name noetl");
    }

    #[test]
    fn render_template_applies_lower_filter() {
        let variables = vars(&[("workload.NAME", "NOETL")]);
        let step_results = HashMap::new();
        let result = render_template(
            "{{ workload.NAME | lower }}",
            &variables,
            &step_results,
        )
        .unwrap();
        assert_eq!(result, "noetl");
    }

    #[test]
    fn render_template_with_result_navigates_json() {
        let variables = HashMap::new();
        let step_results = HashMap::new();
        let json = serde_json::json!({"body": {"name": "noetl"}});
        let result =
            render_template_with_result("name = {{ result.body.name }}", &variables, &step_results, Some(&json))
                .unwrap();
        assert_eq!(result, "name = noetl");
    }

    #[test]
    fn render_template_does_not_reexpand_substituted_values() {
        // A substituted value must never be scanned for further `{{ }}`
        // tokens; otherwise untrusted data could pull in other variables.
        let variables = vars(&[
            ("workload.a", "{{ workload.secret }}"),
            ("workload.secret", "S"),
            ("b", "{{ vars.c }}"),
            ("vars.c", "C"),
        ]);
        let mut step_results = HashMap::new();
        step_results.insert("fetch".to_string(), "{{ workload.secret }}".to_string());
        for _ in 0..50 {
            let result = render_template(
                "x {{ workload.a }} {{ b }} {{ fetch.result }} {{ workload.a | upper }}",
                &variables,
                &step_results,
            )
            .unwrap();
            assert_eq!(
                result,
                "x {{ workload.secret }} {{ vars.c }} {{ workload.secret }} {{ WORKLOAD.SECRET }}"
            );
        }
    }

    #[test]
    fn render_template_leaves_unknown_tokens_untouched() {
        let variables = vars(&[("workload.a", "A")]);
        let step_results = HashMap::new();
        let result = render_template(
            "{{ workload.a }} {{ missing }} {{workload.a}} {{ nope.result }} {{ bad}} {{ workload.a }}",
            &variables,
            &step_results,
        )
        .unwrap();
        assert_eq!(result, "A {{ missing }} {{workload.a}} {{ nope.result }} {{ bad}} A");
    }

    #[test]
    fn render_template_with_result_does_not_reexpand_json_values() {
        let variables = vars(&[("workload.secret", "S"), ("workload.name", "n")]);
        let step_results = HashMap::new();
        let json = serde_json::json!({"body": "{{ workload.secret }}"});
        for _ in 0..50 {
            let result = render_template_with_result(
                "{{ result.body }} {{ workload.name }}",
                &variables,
                &step_results,
                Some(&json),
            )
            .unwrap();
            assert_eq!(result, "{{ workload.secret }} n");
        }
    }

    #[test]
    fn get_json_path_handles_array_index() {
        let json = serde_json::json!({"items": [{"id": 42}, {"id": 99}]});
        assert_eq!(get_json_path(&json, "items[0].id"), serde_json::json!(42));
        assert_eq!(get_json_path(&json, "items[1].id"), serde_json::json!(99));
    }

    #[test]
    fn json_to_rhai_round_trip_through_rhai_to_json_string() {
        let original = serde_json::json!({"key": "value"});
        let rhai = json_to_rhai(&original);
        let stringified = rhai_to_json_string(&rhai);
        // Map ordering isn't guaranteed by rhai::Map, just confirm
        // structural shape (one key, one quoted value).
        assert!(stringified.starts_with('{'));
        assert!(stringified.ends_with('}'));
        assert!(stringified.contains("\"key\""));
        assert!(stringified.contains("\"value\""));
    }
}

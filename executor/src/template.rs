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
pub fn render_template(
    template: &str,
    variables: &HashMap<String, String>,
    step_results: &HashMap<String, String>,
) -> Result<String> {
    let mut result = template.to_string();

    // First, handle templates with filters (e.g., {{ workload.var | lower }}).
    let filter_regex =
        regex::Regex::new(r"\{\{\s*([a-zA-Z_][a-zA-Z0-9_.]*)\s*\|\s*([a-zA-Z_]+)\s*\}\}").unwrap();
    result = filter_regex
        .replace_all(&result, |caps: &regex::Captures| {
            let var_name = &caps[1];
            let filter_name = &caps[2];

            // Try to find the variable value.
            let value = variables
                .get(var_name)
                .or_else(|| variables.get(&format!("workload.{}", var_name)))
                .map(|s| s.as_str())
                .unwrap_or("");

            // Apply the filter.
            match filter_name {
                "lower" => value.to_lowercase(),
                "upper" => value.to_uppercase(),
                "trim" => value.trim().to_string(),
                "default" => {
                    if value.is_empty() {
                        "".to_string()
                    } else {
                        value.to_string()
                    }
                }
                _ => value.to_string(),
            }
        })
        .to_string();

    // Handle workload.* variables.
    for (key, value) in variables {
        if key.starts_with("workload.") {
            let placeholder = format!("{{{{ {} }}}}", key);
            result = result.replace(&placeholder, value);
        }
    }

    // Handle vars.* variables.
    for (key, value) in variables {
        if key.starts_with("vars.") {
            let placeholder = format!("{{{{ {} }}}}", key);
            result = result.replace(&placeholder, value);
        }
    }

    // Handle step_name.result variables.
    for (step_name, value) in step_results {
        let placeholder = format!("{{{{ {}.result }}}}", step_name);
        result = result.replace(&placeholder, value);
    }

    // Also support direct {{ variable }} lookups.
    for (key, value) in variables {
        let placeholder = format!("{{{{ {} }}}}", key);
        result = result.replace(&placeholder, value);
    }

    Ok(result.trim().to_string())
}

/// Render a template that may reference `{{ result.path }}` against the
/// supplied JSON result value (e.g. the previous step's HTTP response
/// body), then apply the regular [`render_template`] pass for the
/// rest of the references.
pub fn render_template_with_result(
    template: &str,
    variables: &HashMap<String, String>,
    step_results: &HashMap<String, String>,
    result_json: Option<&serde_json::Value>,
) -> Result<String> {
    let mut output = template.to_string();

    // Handle result.path expressions like {{ result.status }},
    // {{ result.body.name }}.
    let result_regex = regex::Regex::new(
        r"\{\{\s*result\.([a-zA-Z0-9_.\[\]]+)\s*(?:\|\s*([a-zA-Z_]+(?:\([^)]*\))?))?\s*\}\}",
    )
    .unwrap();

    output = result_regex
        .replace_all(&output, |caps: &regex::Captures| {
            let path = &caps[1];
            let filter = caps.get(2).map(|m| m.as_str());

            if let Some(json) = result_json {
                // Navigate the JSON path.
                let value = get_json_path(json, path);
                let value_str = match &value {
                    serde_json::Value::String(s) => s.clone(),
                    serde_json::Value::Number(n) => n.to_string(),
                    serde_json::Value::Bool(b) => b.to_string(),
                    serde_json::Value::Null => "".to_string(),
                    other => other.to_string(),
                };

                // Apply filter if present.
                if let Some(f) = filter {
                    if f == "default" || f.starts_with("default(") {
                        if value_str.is_empty() || value_str == "null" {
                            // Extract default value from default('value') or default("").
                            if let Some(start) = f.find('(') {
                                let inner = &f[start + 1..f.len() - 1];
                                inner.trim_matches(|c| c == '\'' || c == '"').to_string()
                            } else {
                                "".to_string()
                            }
                        } else {
                            value_str
                        }
                    } else {
                        value_str
                    }
                } else {
                    value_str
                }
            } else {
                "".to_string()
            }
        })
        .to_string();

    // Then apply normal template rendering for other variables.
    render_template(&output, variables, step_results)
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

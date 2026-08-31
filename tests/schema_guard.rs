//! Guards for the generated playbook JSON Schema (noetl/ai-meta#201).
//!
//! A schema nothing checks against is the dead-code trap in another costume: it
//! keeps being published, keeps looking authoritative, and silently stops
//! describing the model it was generated from. These tests are the forcing
//! function.

use std::path::PathBuf;

fn committed() -> serde_json::Value {
    let p = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("schema/playbook.schema.json");
    let text = std::fs::read_to_string(&p)
        .unwrap_or_else(|e| panic!("cannot read {}: {e}", p.display()));
    serde_json::from_str(&text).expect("committed schema is valid JSON")
}

/// The committed artifact must equal what the model generates right now.
///
/// If this fails the model changed and the schema was not regenerated:
///     cargo run -- schema --output schema/playbook.schema.json
#[test]
fn committed_schema_is_not_stale() {
    let generated = noetl_executor::playbook::playbook_json_schema();
    assert_eq!(
        committed(),
        generated,
        "schema/playbook.schema.json is STALE — the playbook model changed \
         without regenerating it. Run:\n    \
         cargo run -- schema --output schema/playbook.schema.json"
    );
}

/// Shape contract with the schema this replaces (the retired Python
/// `_generate_schema` output).
#[test]
fn schema_matches_the_reference_shape() {
    let s = committed();
    assert_eq!(
        s["$schema"], "https://json-schema.org/draft/2020-12/schema",
        "draft must stay 2020-12 — editors and the reference schema assume it"
    );
    assert_eq!(s["type"], "object");

    let mut required: Vec<&str> = s["required"]
        .as_array()
        .expect("required is an array")
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    required.sort();
    assert_eq!(
        required,
        vec!["apiVersion", "kind", "metadata", "workflow"],
        "the required set is a compatibility contract with the previous schema"
    );

    let mut props: Vec<&str> = s["properties"]
        .as_object()
        .expect("properties is an object")
        .keys()
        .map(|k| k.as_str())
        .collect();
    props.sort();
    assert_eq!(
        props,
        vec![
            "apiVersion", "executor", "keychain", "kind", "metadata", "workbook",
            "workflow", "workload"
        ],
        "root property set must match the reference schema"
    );

    assert!(
        s.get("additionalProperties").is_none(),
        "the schema must stay PERMISSIVE: the parser ignores unknown keys, so a \
         schema that rejected them would fail playbooks that run fine"
    );
}

/// The schema must actually constrain. A guard never shown to fail is
/// indistinguishable from a guard that cannot fail.
#[test]
fn schema_constrains_rather_than_rubber_stamps() {
    let s = committed();
    let defs = &s["$defs"];

    for (ty, key) in [("ToolSpec", "kind"), ("Step", "step"), ("LoopConfig", "iterator")] {
        assert!(
            defs[ty]["required"]
                .as_array()
                .map(|a| a.iter().any(|v| v == key))
                .unwrap_or(false),
            "{ty} must require `{key}`, or that part of a playbook is unconstrained"
        );
    }
    assert_eq!(
        defs["CursorSpec"]["required"].as_array().map(|a| a.len()),
        Some(3),
        "CursorSpec must require kind/auth/claim"
    );

    let lm = &defs["LoopMode"];
    assert!(
        lm.get("oneOf").is_some() || lm.get("enum").is_some(),
        "LoopMode must be a closed enumeration, or `mode: sideways` would validate"
    );
}

/// A numeric DSL field must accept a template expression, because real
/// playbooks write `lease_seconds: '{{ frame_lease_seconds }}'`. Typing these
/// as plain numbers broke both parsing and validation before `TemplatableF64`.
#[test]
fn numeric_fields_accept_templates() {
    let s = committed();
    // The field is `anyOf[$ref TemplatableF64, null]`, so follow the ref rather
    // than inspecting the wrapper — checking the wrapper passes for the wrong
    // reason and would keep passing if the target lost its string branch.
    let field = &s["$defs"]["FramePolicy"]["properties"]["lease_seconds"];
    let refs: Vec<&str> = field["anyOf"]
        .as_array()
        .expect("lease_seconds is an anyOf")
        .iter()
        .filter_map(|b| b.get("$ref").and_then(|r| r.as_str()))
        .collect();
    assert_eq!(
        refs,
        vec!["#/$defs/TemplatableF64"],
        "lease_seconds must resolve through TemplatableF64"
    );

    let target = serde_json::to_string(&s["$defs"]["TemplatableF64"]).unwrap();
    assert!(
        target.contains("\"type\":\"number\"") && target.contains("\"type\":\"string\""),
        "TemplatableF64 must accept BOTH a number and a template string; got {target}"
    );
}

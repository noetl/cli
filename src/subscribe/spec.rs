//! Parse a `kind: Subscription` catalog spec for local-mode execution
//! (RFC #90 Phase 6).
//!
//! This is the local-mode sibling of the worker runtime's `parse_spec`
//! (`repos/worker/src/subscription.rs`).  It extracts the same fields — source
//! connection, dispatch target, header-directive allowlist, spool block — but
//! constrains the spool to the **`local_disk`** backend (RFC §8.6: local mode's
//! only spool backend) and treats `dispatch.playbook` as a file path / catalog
//! ref the local dispatcher resolves, not a server-side catalog path.

use anyhow::{Context, Result};
use noetl_tools::spool::{SpoolBackendKind, SpoolSpec};
use noetl_tools::tools::source::DirectiveSpec;
use noetl_tools::tools::SubscriptionConfig;

/// Default poll batch for the local drain loop (matches the bounded-drain tool).
const RUNTIME_BATCH_DEFAULT: u32 = 100;

/// The fields the local runtime needs from a `kind: Subscription` spec.
#[derive(Debug, Clone)]
pub struct ParsedSpec {
    /// Connection config for [`noetl_tools::tools::build_source`].
    pub source_cfg: SubscriptionConfig,
    /// Optional credential alias (resolved from a local credential file, never
    /// a server round-trip in local mode).
    pub auth_alias: Option<String>,
    /// Target playbook run per message — a file path or catalog ref the local
    /// dispatcher resolves.
    pub default_playbook: String,
    /// Which part of the normalized message becomes the playbook body.
    pub payload_from: String,
    /// Default downstream pool label (matched against declared spool downstreams).
    pub default_pool: Option<String>,
    /// Header-directive allowlist (RFC §7).
    pub directives: DirectiveSpec,
    /// Store-and-forward spool config (RFC §8) — `local_disk` only in local mode.
    pub spool: SpoolSpec,
    /// Subscription path / name (for the event envelope + spool item subject).
    pub path: String,
    /// Poll batch size.
    pub batch: u32,
    /// Poll wait.
    pub timeout_ms: Option<u64>,
}

/// Parse a `kind: Subscription` YAML into the local [`ParsedSpec`].
///
/// `path` is the subscription's identity (its file path or catalog name) — it
/// rides every event + spool item so the local trail is attributable.
/// `spool_dir_override` (from `--spool-dir`) wins over the spec's
/// `spool.path`; when neither is set and the spool buffers, a default under
/// the events directory is used by the caller.
pub fn parse_spec(
    yaml: &serde_yaml::Value,
    path: &str,
    spool_dir_override: Option<&str>,
) -> Result<ParsedSpec> {
    let kind = yaml.get("kind").and_then(|v| v.as_str()).unwrap_or("");
    if kind != "Subscription" {
        anyhow::bail!(
            "expected 'kind: Subscription', got '{}' — `noetl subscribe` runs a kind:Subscription spec",
            kind
        );
    }
    let spec = yaml.get("spec").context("subscription YAML missing 'spec'")?;

    let source = spec
        .get("source")
        .and_then(|v| v.as_str())
        .context("subscription spec missing 'source'")?
        .to_string();

    let auth_alias = spec.get("auth").and_then(|v| v.as_str()).map(str::to_string);

    // Connection config: source + every connection key SubscriptionConfig knows.
    let mut cfg = serde_json::Map::new();
    cfg.insert("source".to_string(), serde_json::json!(source));
    for key in [
        "url", "user", "password", "token", "stream", "consumer", "subscription", "endpoint",
        "topic", "group", "brokers",
    ] {
        if let Some(v) = spec.get(key) {
            cfg.insert(key.to_string(), serde_json::to_value(v)?);
        }
    }
    if let Some(alias) = &auth_alias {
        cfg.insert("auth".to_string(), serde_json::json!(alias));
    }
    let source_cfg: SubscriptionConfig = serde_json::from_value(serde_json::Value::Object(cfg))
        .context("subscription spec did not yield a valid source config")?;

    // runtime knobs.
    let runtime = spec.get("runtime");
    let batch = runtime
        .and_then(|r| r.get("batch"))
        .and_then(|v| v.as_u64())
        .map(|b| b as u32)
        .unwrap_or(RUNTIME_BATCH_DEFAULT);
    let timeout_ms = runtime
        .and_then(|r| r.get("timeout_ms"))
        .and_then(|v| v.as_u64());

    // dispatch block.
    let dispatch = spec.get("dispatch").context("subscription spec missing 'dispatch'")?;
    let default_playbook = dispatch
        .get("playbook")
        .and_then(|v| v.as_str())
        .context("subscription spec missing 'dispatch.playbook'")?
        .to_string();
    let payload_from = dispatch
        .get("payload_from")
        .and_then(|v| v.as_str())
        .unwrap_or("message.json")
        .to_string();
    let default_pool = dispatch
        .get("execution_pool")
        .and_then(|v| v.as_str())
        .map(str::to_string);

    // headers (directive allowlist) — optional.
    let directives = match spec.get("headers") {
        Some(h) => {
            let json = serde_json::to_value(h)
                .context("subscription spec 'headers' is not serializable")?;
            DirectiveSpec::parse(&json)
                .map_err(|e| anyhow::anyhow!("invalid subscription 'headers' block: {e}"))?
        }
        None => DirectiveSpec::default(),
    };

    // spool block — optional; absent → off.  Local mode only supports the
    // `local_disk` backend (RFC §8.6); rewrite the backend + path so a spec
    // authored for the in-cluster `nats_object`/`gcs` backend still runs
    // locally (the engine + ordering + circuit logic are backend-agnostic).
    let spool = match spec.get("spool") {
        Some(s) => {
            let mut json = serde_json::to_value(s)
                .context("subscription spec 'spool' is not serializable")?;
            localize_spool(&mut json, spool_dir_override, path);
            SpoolSpec::parse(Some(&json))
                .map_err(|e| anyhow::anyhow!("invalid subscription 'spool' block: {e}"))?
        }
        None if spool_dir_override.is_some() => {
            // `--spool-dir` with no spec block → buffer_and_ack/local_disk default.
            let json = serde_json::json!({
                "mode": "buffer_and_ack",
                "backend": "local_disk",
                "path": spool_dir_override.unwrap(),
            });
            SpoolSpec::parse(Some(&json))
                .map_err(|e| anyhow::anyhow!("invalid --spool-dir default: {e}"))?
        }
        None => SpoolSpec::off(),
    };

    // Belt-and-suspenders: the localizer should have forced this already.
    if spool.buffers() && spool.backend != SpoolBackendKind::LocalDisk {
        anyhow::bail!(
            "local mode supports only the 'local_disk' spool backend (got '{}'); \
             omit `backend:` or set it to local_disk",
            spool.backend.as_str()
        );
    }

    Ok(ParsedSpec {
        source_cfg,
        auth_alias,
        default_playbook,
        payload_from,
        default_pool,
        directives,
        spool,
        path: path.to_string(),
        batch,
        timeout_ms,
    })
}

/// Rewrite a spool JSON block for local execution: force `backend: local_disk`
/// and resolve a concrete `path` (CLI override > spec path > a per-subscription
/// default under `./.noetl-spool/`).
fn localize_spool(json: &mut serde_json::Value, dir_override: Option<&str>, path: &str) {
    let Some(map) = json.as_object_mut() else {
        return;
    };
    map.insert("backend".to_string(), serde_json::json!("local_disk"));
    let resolved = dir_override
        .map(str::to_string)
        .or_else(|| map.get("path").and_then(|v| v.as_str()).map(str::to_string))
        .unwrap_or_else(|| format!(".noetl-spool/{}", slug(path)));
    map.insert("path".to_string(), serde_json::json!(resolved));
}

/// Filesystem-safe slug of a subscription path for the default spool dir.
fn slug(path: &str) -> String {
    path.chars()
        .map(|c| if c.is_alphanumeric() { c } else { '_' })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn yaml(s: &str) -> serde_yaml::Value {
        serde_yaml::from_str(s).unwrap()
    }

    #[test]
    fn parses_nats_source_and_dispatch() {
        let spec = parse_spec(
            &yaml(
                r#"
kind: Subscription
spec:
  source: nats
  url: nats://localhost:4222
  stream: ORDERS
  consumer: orders-local
  runtime: { batch: 25, timeout_ms: 2000 }
  dispatch: { playbook: ./playbooks/process.yaml, execution_pool: local }
"#,
            ),
            "subscriptions/orders",
            None,
        )
        .unwrap();
        assert_eq!(spec.source_cfg.source, "nats");
        assert_eq!(spec.source_cfg.stream.as_deref(), Some("ORDERS"));
        assert_eq!(spec.default_playbook, "./playbooks/process.yaml");
        assert_eq!(spec.default_pool.as_deref(), Some("local"));
        assert_eq!(spec.batch, 25);
        assert_eq!(spec.timeout_ms, Some(2000));
        assert!(!spec.spool.buffers());
    }

    #[test]
    fn rejects_non_subscription_kind() {
        let err = parse_spec(&yaml("kind: Playbook\nspec: {}\n"), "p", None).unwrap_err();
        assert!(format!("{err}").contains("kind: Subscription"));
    }

    #[test]
    fn requires_dispatch_playbook() {
        let err = parse_spec(
            &yaml("kind: Subscription\nspec:\n  source: nats\n  stream: S\n  consumer: C\n  dispatch: {}\n"),
            "p",
            None,
        )
        .unwrap_err();
        assert!(format!("{err}").contains("dispatch.playbook"));
    }

    #[test]
    fn forces_local_disk_backend_even_when_spec_says_nats_object() {
        // A spec authored for the in-cluster nats_object backend still runs
        // locally — the localizer rewrites backend + path.
        let spec = parse_spec(
            &yaml(
                r#"
kind: Subscription
spec:
  source: nats
  stream: IOT
  consumer: iot-local
  dispatch: { playbook: ./ingest.yaml, execution_pool: warehouse }
  spool:
    mode: buffer_and_ack
    backend: nats_object
    bucket: noetl_spool_iot
    ordering: per_key
    ordering_key: device_id
    circuit:
      trip_after: 2
      probe_after_ms: 500
      probe_interval_ms: 300
      downstream:
        - { name: warehouse, type: http, target: "http://127.0.0.1:9/health" }
"#,
            ),
            "subscriptions/iot",
            None,
        )
        .unwrap();
        assert!(spec.spool.buffers());
        assert_eq!(spec.spool.backend, SpoolBackendKind::LocalDisk);
        assert!(spec.spool.path.is_some(), "a concrete local path was resolved");
        assert_eq!(spec.spool.ordering_key.as_deref(), Some("device_id"));
        assert_eq!(spec.spool.circuit.downstream.len(), 1);
    }

    #[test]
    fn spool_dir_override_wins() {
        let spec = parse_spec(
            &yaml(
                r#"
kind: Subscription
spec:
  source: nats
  stream: S
  consumer: C
  dispatch: { playbook: ./p.yaml }
  spool: { mode: buffer_and_ack, backend: local_disk, path: /tmp/from-spec }
"#,
            ),
            "p",
            Some("/tmp/from-cli"),
        )
        .unwrap();
        assert_eq!(spec.spool.path.as_deref(), Some("/tmp/from-cli"));
    }
}

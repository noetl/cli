//! `noetl subscribe` — run a `kind: Subscription` listener standalone in local
//! mode (RFC #90 Phase 6, §5.3).
//!
//! No Kubernetes, no NATS-dispatch server is required for the listening
//! itself: this reuses the **same** `SourceClient` poll + header-directive
//! engine + store-and-forward spool engine the in-cluster worker runtime uses
//! (`noetl_tools::tools::source` + `noetl_tools::spool`), and emits the **same**
//! `ExecutorEvent` envelope — to a local `FileEventSink` (JSONL) — so a local
//! run produces a replayable event-sourced log identical in shape to the
//! in-cluster / Cloud Run trail.  Per RFC §5.3 a received message either runs
//! the target playbook **in-process** (the pure-local default, via
//! `PlaybookRunner`) or POSTs `/api/execute` when `--dispatch server` is set.

mod dispatch;
mod runtime;
mod sink;
mod spec;
mod spool;

#[cfg(test)]
mod tests;

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use noetl_events::EventSink;

use dispatch::{Dispatcher, LocalDispatcher, ServerDispatcher};
use runtime::{LocalRuntime, StopWhen};
use sink::{FileEventSink, LocalIdGen};
use spec::parse_spec;

/// Parsed `noetl subscribe` invocation (built from the clap subcommand).
#[derive(Debug, Clone)]
pub struct SubscribeArgs {
    /// Path to a `kind: Subscription` YAML spec.
    pub reference: String,
    /// Dispatch model: `local` (in-process) or `server` (`POST /api/execute`).
    pub dispatch: String,
    /// Server URL for `--dispatch server` (defaults to the resolved base URL).
    pub server_url: Option<String>,
    /// JSONL event-sink path (defaults to `./<name>-events.jsonl`).
    pub events: Option<PathBuf>,
    /// Override the spool dir (`local_disk` backend path).
    pub spool_dir: Option<String>,
    /// Base dir for resolving relative `dispatch.playbook` refs (defaults to
    /// the spec's directory).
    pub playbook_dir: Option<PathBuf>,
    /// Local credential JSON file injected for the source's `auth:` alias.
    pub credential: Option<PathBuf>,
    /// Stop after N handled messages (`0` = run continuously).
    pub max_messages: u64,
    /// Drain the source once then exit.
    pub once: bool,
    /// Verbose dispatch output.
    pub verbose: bool,
}

/// Entry point dispatched from `main`.
pub async fn run(args: SubscribeArgs) -> Result<()> {
    // A long-lived listener needs its diagnostics visible: install a tracing
    // subscriber (stderr) honoring `RUST_LOG` (default `info`).  `try_init`
    // is a no-op if the process already installed one.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .try_init();

    // 1. Load + parse the subscription spec.
    let spec_path = PathBuf::from(&args.reference);
    let yaml_str = std::fs::read_to_string(&spec_path)
        .with_context(|| format!("read subscription spec {}", spec_path.display()))?;
    let yaml: serde_yaml::Value = serde_yaml::from_str(&yaml_str)
        .with_context(|| format!("parse subscription YAML {}", spec_path.display()))?;

    let sub_name = yaml
        .get("metadata")
        .and_then(|m| m.get("name"))
        .and_then(|v| v.as_str())
        .map(str::to_string)
        .unwrap_or_else(|| {
            spec_path
                .file_stem()
                .map(|s| s.to_string_lossy().to_string())
                .unwrap_or_else(|| "subscription".to_string())
        });

    let parsed = parse_spec(&yaml, &sub_name, args.spool_dir.as_deref())?;

    // 2. Build the local FileEventSink (the replayable trail).
    let events_path = args.events.clone().unwrap_or_else(|| {
        PathBuf::from(format!("{}-events.jsonl", slug(&sub_name)))
    });
    let sink: Arc<dyn EventSink> =
        Arc::new(FileEventSink::create(&events_path, true).context("create event sink")?);

    // 3. Build the dispatcher per the RFC §5.3 local dispatch model.
    let ids = Arc::new(LocalIdGen::new());
    let dispatcher: Arc<dyn Dispatcher> = match args.dispatch.as_str() {
        "server" => {
            let url = args
                .server_url
                .clone()
                .context("--dispatch server requires --server-url")?;
            Arc::new(ServerDispatcher::new(url))
        }
        "local" => {
            let playbook_dir = args.playbook_dir.clone().unwrap_or_else(|| {
                spec_path
                    .parent()
                    .map(PathBuf::from)
                    .unwrap_or_else(|| PathBuf::from("."))
            });
            Arc::new(LocalDispatcher::new(playbook_dir, args.verbose, ids.clone()))
        }
        other => anyhow::bail!("unknown --dispatch '{other}' (use 'local' or 'server')"),
    };

    // 4. Resolve a local credential for the source's auth alias (no server call).
    let credential = match (&parsed.auth_alias, &args.credential) {
        (Some(alias), Some(file)) => {
            let json = std::fs::read_to_string(file)
                .with_context(|| format!("read credential file {}", file.display()))?;
            Some((alias.clone(), json))
        }
        _ => None,
    };

    // 5. Stop condition.
    let stop = if args.once {
        StopWhen::OneDrain
    } else if args.max_messages > 0 {
        StopWhen::Handled(args.max_messages)
    } else {
        StopWhen::Never
    };

    eprintln!("🔔 noetl subscribe — {} (source: {})", sub_name, parsed.source_cfg.source);
    eprintln!("   dispatch : {}", dispatcher.label());
    eprintln!("   events   : {}", events_path.display());
    if parsed.spool.buffers() {
        eprintln!(
            "   spool    : local_disk {} (mode: {})",
            parsed.spool.path.as_deref().unwrap_or("?"),
            parsed.spool.mode.as_str()
        );
    }

    // 6. Run with a Ctrl-C / SIGTERM shutdown.
    let rt = LocalRuntime::new(parsed, sink, dispatcher, ids, credential, stop);
    let summary = rt.run(shutdown_signal()).await?;

    eprintln!(
        "✓ subscription stopped — received={} dispatched={} spooled={} replayed={} failed={} pending_spooled={}",
        summary.received,
        summary.dispatched,
        summary.spooled,
        summary.replayed,
        summary.failed,
        summary.pending_spooled,
    );
    eprintln!("  event-sourced trail: {}", events_path.display());
    Ok(())
}

/// Resolve on Ctrl-C (SIGINT) or SIGTERM (the K8s/Cloud-Run termination shape,
/// kept consistent so the same drain-on-shutdown contract holds locally).
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut term = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
                return;
            }
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

fn slug(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_alphanumeric() || c == '-' { c } else { '_' })
        .collect()
}

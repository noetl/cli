//! `noetl buffer …` and `noetl push` — the CLI surface of the R-1.4 local
//! Feather event buffer (blueprint § H.11, [`crate::local_buffer`]).
//!
//! `buffer` is how shell steps (and any other process) write to and read from
//! `<root>/<execution_id>/events.feather`; `push` is the § H.11.5 bridge that
//! ships a closed buffer to `POST /api/events/batch` so it lands on the EHDB
//! event log once a control plane exists.

use std::collections::BTreeMap;
use std::io::{BufRead, Read};
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use clap::{Args, Subcommand};
use reqwest::Client;

use crate::local_buffer::{self as lb, CreateOptions, LocalEventBuffer, NewEvent};

#[derive(Args, Debug, Clone)]
pub struct BufferRoot {
    /// Buffer root (default: $NOETL_LOCAL_DIR, else ~/.noetl/local)
    #[arg(long, global = true)]
    pub dir: Option<PathBuf>,
}

impl BufferRoot {
    fn root(&self) -> PathBuf {
        match &self.dir {
            Some(d) => PathBuf::from(shellexpand::tilde(&d.to_string_lossy()).to_string()),
            None => lb::default_root(),
        }
    }
}

#[derive(Subcommand, Debug)]
pub enum BufferCommand {
    /// Start a new execution buffer; prints the execution id
    Init {
        /// Use this execution id instead of allocating one
        #[arg(long)]
        execution_id: Option<i64>,
        /// Playbook path recorded in metadata.json
        #[arg(long, default_value = "")]
        playbook: String,
        /// Ownership stack this execution's provider facts belong to
        #[arg(long)]
        stack: Option<String>,
        /// Extra metadata labels (key=value, repeatable)
        #[arg(long = "label")]
        labels: Vec<String>,
        /// Print the metadata as JSON instead of the bare id
        #[arg(long)]
        json: bool,
    },
    /// Append events to an open execution buffer
    ///
    /// Either one event from flags, or `--jsonl -`: one JSON object per line
    /// ({event_type, step, status, context, result, meta, parent_event_id}).
    #[command(verbatim_doc_comment)]
    Append {
        execution_id: i64,
        #[arg(long)]
        event_type: Option<String>,
        #[arg(long, default_value = "")]
        step: String,
        #[arg(long, default_value = "COMPLETED")]
        status: String,
        /// Result JSON (`-` reads stdin)
        #[arg(long)]
        result_json: Option<String>,
        #[arg(long)]
        context_json: Option<String>,
        #[arg(long)]
        meta_json: Option<String>,
        /// Read events as JSON lines from this file (`-` = stdin)
        #[arg(long, conflicts_with_all = ["event_type", "result_json", "context_json", "meta_json"])]
        jsonl: Option<String>,
    },
    /// Flush and mark an execution COMPLETED or FAILED (immutable afterwards)
    Close {
        execution_id: i64,
        #[arg(long, default_value = "COMPLETED")]
        status: String,
    },
    /// List execution buffers, oldest first
    List {
        #[arg(long)]
        stack: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Decode events back to JSON lines (one execution, or every execution of a stack)
    Cat {
        execution_id: Option<i64>,
        #[arg(long, conflicts_with = "execution_id")]
        stack: Option<String>,
        /// Include executions still RUNNING (default: closed ones only, plus
        /// an explicitly named execution in any state)
        #[arg(long)]
        include_running: bool,
        /// Only events of this type (e.g. provider.fact)
        #[arg(long)]
        event_type: Option<String>,
    },
}

#[derive(Args, Debug)]
pub struct PushArgs {
    /// Execution ids to push (default with --all: every closed, unpushed one)
    pub execution_ids: Vec<i64>,
    /// Push every closed execution not yet pushed, oldest first
    #[arg(long)]
    pub all: bool,
    /// With --all: only this stack's executions
    #[arg(long, requires = "all")]
    pub stack: Option<String>,
    /// Push again even if metadata.json records an earlier push
    #[arg(long)]
    pub force: bool,
    /// Show what would be pushed without sending anything
    #[arg(long)]
    pub dry_run: bool,
    /// Server base URL (default: the CLI's resolved server)
    #[arg(long)]
    pub server: Option<String>,
    #[command(flatten)]
    pub root: BufferRoot,
}

fn parse_json_arg(raw: Option<&str>, what: &str) -> Result<serde_json::Value> {
    match raw {
        None => Ok(serde_json::Value::Null),
        Some("-") => {
            let mut s = String::new();
            std::io::stdin().read_to_string(&mut s)?;
            serde_json::from_str(&s).with_context(|| format!("{what} from stdin is not JSON"))
        }
        Some(s) => serde_json::from_str(s).with_context(|| format!("{what} is not JSON")),
    }
}

fn read_jsonl(src: &str) -> Result<Vec<NewEvent>> {
    let reader: Box<dyn BufRead> = if src == "-" {
        Box::new(std::io::BufReader::new(std::io::stdin()))
    } else {
        Box::new(std::io::BufReader::new(
            std::fs::File::open(src).with_context(|| format!("open {src}"))?,
        ))
    };
    let mut out = Vec::new();
    for (i, line) in reader.lines().enumerate() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        out.push(serde_json::from_str(&line).with_context(|| format!("JSONL line {}", i + 1))?);
    }
    Ok(out)
}

fn parse_labels(raw: &[String]) -> Result<BTreeMap<String, String>> {
    raw.iter()
        .map(|kv| {
            kv.split_once('=')
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .with_context(|| format!("label `{kv}` is not key=value"))
        })
        .collect()
}

pub fn run_buffer(root: &BufferRoot, command: BufferCommand) -> Result<()> {
    let root = root.root();
    match command {
        BufferCommand::Init {
            execution_id,
            playbook,
            stack,
            labels,
            json,
        } => {
            let b = LocalEventBuffer::create(
                &root,
                CreateOptions {
                    execution_id,
                    playbook_path: playbook,
                    stack,
                    labels: parse_labels(&labels)?,
                },
            )?;
            if json {
                let m = lb::read_metadata(b.dir())?;
                println!("{}", serde_json::to_string(&m)?);
            } else {
                println!("{}", b.execution_id());
            }
        }
        BufferCommand::Append {
            execution_id,
            event_type,
            step,
            status,
            result_json,
            context_json,
            meta_json,
            jsonl,
        } => {
            let events = match jsonl {
                Some(src) => read_jsonl(&src)?,
                None => {
                    let Some(event_type) = event_type else {
                        bail!("give --event-type (one event) or --jsonl (many)");
                    };
                    vec![NewEvent {
                        event_type,
                        step,
                        status,
                        result: parse_json_arg(result_json.as_deref(), "--result-json")?,
                        context: parse_json_arg(context_json.as_deref(), "--context-json")?,
                        meta: parse_json_arg(meta_json.as_deref(), "--meta-json")?,
                        ..Default::default()
                    }]
                }
            };
            let mut b = LocalEventBuffer::open(&root, execution_id)?;
            let n = events.len();
            for e in events {
                b.append(e)?;
            }
            b.flush()?;
            eprintln!("buffered {n} event(s) → {}", b.dir().join(lb::EVENTS_FILE).display());
        }
        BufferCommand::Close { execution_id, status } => {
            let b = LocalEventBuffer::open(&root, execution_id)?;
            let m = b.close(&status)?;
            println!("{}", serde_json::to_string(&m)?);
        }
        BufferCommand::List { stack, json } => {
            let rows: Vec<_> = lb::list(&root)?
                .into_iter()
                .filter(|(_, m)| stack.is_none() || m.stack == stack)
                .collect();
            if json {
                let v: Vec<serde_json::Value> = rows
                    .iter()
                    .map(|(d, m)| {
                        let mut j = serde_json::to_value(m).unwrap_or_default();
                        j["dir"] = serde_json::json!(d.display().to_string());
                        j
                    })
                    .collect();
                println!("{}", serde_json::to_string(&v)?);
            } else {
                for (_, m) in rows {
                    println!(
                        "{}\t{}\t{}\t{} event(s)\t{}\t{}",
                        m.execution_id,
                        m.status,
                        m.stack.as_deref().unwrap_or("-"),
                        m.event_count,
                        m.started_at.to_rfc3339(),
                        if m.pushed_at.is_some() { "pushed" } else { "local" }
                    );
                }
            }
        }
        BufferCommand::Cat {
            execution_id,
            stack,
            include_running,
            event_type,
        } => {
            let targets: Vec<(PathBuf, lb::Metadata)> = match execution_id {
                Some(eid) => {
                    let d = lb::execution_dir(&root, eid);
                    vec![(d.clone(), lb::read_metadata(&d)?)]
                }
                None => lb::list(&root)?
                    .into_iter()
                    .filter(|(_, m)| stack.is_none() || m.stack == stack)
                    .filter(|(_, m)| include_running || m.status != "RUNNING")
                    .collect(),
            };
            use std::io::Write;
            let out = std::io::stdout();
            let mut out = out.lock();
            for (d, m) in targets {
                for e in lb::read_events(&d)? {
                    if event_type.as_deref().is_some_and(|t| t != e.event_type) {
                        continue;
                    }
                    writeln!(out, "{}", lb::event_json(&e, m.stack.as_deref()))?;
                }
            }
        }
    }
    Ok(())
}

/// What a push of one execution sends (§ H.11.5).
pub struct PushRequest {
    pub execution_id: i64,
    pub event_count: usize,
    pub body: Vec<u8>,
}

/// Build the § H.11.5 request body for one closed execution.
pub fn build_push(dir: &Path) -> Result<PushRequest> {
    let m = lb::read_metadata(dir)?;
    if m.status == "RUNNING" {
        bail!("execution {} is still RUNNING; close it before pushing", m.execution_id);
    }
    let body = std::fs::read(dir.join(lb::EVENTS_FILE))
        .with_context(|| format!("read {}", dir.join(lb::EVENTS_FILE).display()))?;
    // Decode before sending: a corrupt file fails here, locally, with a path.
    let event_count = lb::decode_feather(&body)?.len();
    Ok(PushRequest {
        execution_id: m.execution_id,
        event_count,
        body,
    })
}

pub async fn run_push(client: &Client, base_url: &str, args: PushArgs) -> Result<()> {
    let root = args.root.root();
    let server = args
        .server
        .as_deref()
        .unwrap_or(base_url)
        .trim_end_matches('/')
        .to_string();
    let url = format!("{server}/api/events/batch");

    let targets: Vec<(PathBuf, lb::Metadata)> = if args.all {
        if !args.execution_ids.is_empty() {
            bail!("give execution ids or --all, not both");
        }
        lb::list(&root)?
            .into_iter()
            .filter(|(_, m)| args.stack.is_none() || m.stack == args.stack)
            .filter(|(_, m)| m.status != "RUNNING")
            .filter(|(_, m)| args.force || m.pushed_at.is_none())
            .collect()
    } else {
        if args.execution_ids.is_empty() {
            bail!("give one or more execution ids, or --all");
        }
        let mut v = Vec::new();
        for eid in &args.execution_ids {
            let d = lb::execution_dir(&root, *eid);
            let m = lb::read_metadata(&d)?;
            if m.pushed_at.is_some() && !args.force {
                eprintln!(
                    "skip {eid}: already pushed to {} (use --force)",
                    m.pushed_to.as_deref().unwrap_or("?")
                );
                continue;
            }
            v.push((d, m));
        }
        // Oldest first regardless of argument order: the server appends in
        // arrival order and a provider_fact fold is order-sensitive.
        v.sort_by_key(|(_, m)| m.execution_id);
        v
    };

    if targets.is_empty() {
        eprintln!("nothing to push");
        return Ok(());
    }

    for (dir, _) in targets {
        let req = build_push(&dir)?;
        if args.dry_run {
            println!(
                "{}",
                serde_json::json!({"execution_id": req.execution_id.to_string(), "events": req.event_count,
                                   "bytes": req.body.len(), "url": url, "dry_run": true})
            );
            continue;
        }
        let resp = client
            .post(&url)
            .header(reqwest::header::CONTENT_TYPE, lb::MEDIA_TYPE)
            .header("X-NoETL-Execution-Id", req.execution_id.to_string())
            .header("X-NoETL-Event-Count", req.event_count.to_string())
            .body(req.body)
            .send()
            .await
            .with_context(|| format!("POST {url}"))?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            // Stop at the first failure: pushing a later execution past a
            // failed earlier one would reorder the event log.
            bail!(
                "push of execution {} failed: {status}: {}",
                req.execution_id,
                text.chars().take(600).collect::<String>()
            );
        }
        let mut m = lb::read_metadata(&dir)?;
        m.pushed_at = Some(chrono::Utc::now());
        m.pushed_to = Some(server.clone());
        lb::write_metadata(&dir, &m)?;
        println!("{text}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn push_refuses_running_and_builds_exact_bytes() {
        let root = std::env::temp_dir().join(format!("noetl-push-{}", lb::new_id()));
        let mut b = LocalEventBuffer::create(&root, CreateOptions::default()).unwrap();
        let eid = b.execution_id();
        b.append(NewEvent {
            event_type: "step.exit".into(),
            step: "a".into(),
            ..Default::default()
        })
        .unwrap();
        b.flush().unwrap();
        let dir = lb::execution_dir(&root, eid);
        assert!(build_push(&dir).is_err(), "a RUNNING buffer must not be pushed");
        b.close("COMPLETED").unwrap();
        let req = build_push(&dir).unwrap();
        assert_eq!(req.execution_id, eid);
        assert_eq!(req.event_count, 1);
        assert_eq!(
            req.body,
            std::fs::read(dir.join(lb::EVENTS_FILE)).unwrap(),
            "the file is sent verbatim"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    fn closed_buffer(root: &Path, stack: &str, n: usize) -> i64 {
        let mut b = LocalEventBuffer::create(
            root,
            CreateOptions {
                stack: Some(stack.into()),
                ..Default::default()
            },
        )
        .unwrap();
        for i in 0..n {
            b.append(NewEvent {
                event_type: "provider.fact".into(),
                step: format!("s{i}"),
                ..Default::default()
            })
            .unwrap();
        }
        let eid = b.execution_id();
        b.close("COMPLETED").unwrap();
        eid
    }

    #[tokio::test]
    async fn push_sends_h11_5_contract_oldest_first_once() {
        use wiremock::matchers::{header, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/events/batch"))
            .and(header("content-type", lb::MEDIA_TYPE))
            .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"status":"ok"}"#))
            .mount(&server)
            .await;
        let root = std::env::temp_dir().join(format!("noetl-push2-{}", lb::new_id()));
        let e1 = closed_buffer(&root, "s", 2);
        let e2 = closed_buffer(&root, "s", 3);
        let _other = closed_buffer(&root, "other", 1);
        let args = |all: bool, ids: Vec<i64>| PushArgs {
            execution_ids: ids,
            all,
            stack: all.then(|| "s".to_string()),
            force: false,
            dry_run: false,
            server: Some(server.uri()),
            root: BufferRoot {
                dir: Some(root.clone()),
            },
        };
        run_push(&Client::new(), "http://unused", args(true, vec![]))
            .await
            .unwrap();
        let reqs = server.received_requests().await.unwrap();
        assert_eq!(reqs.len(), 2, "only stack `s`");
        let hdr = |r: &wiremock::Request, k: &str| r.headers.get(k).unwrap().to_str().unwrap().to_string();
        assert_eq!(hdr(&reqs[0], "x-noetl-execution-id"), e1.to_string(), "oldest first");
        assert_eq!(hdr(&reqs[0], "x-noetl-event-count"), "2");
        assert_eq!(hdr(&reqs[1], "x-noetl-execution-id"), e2.to_string());
        assert_eq!(
            reqs[1].body,
            std::fs::read(lb::execution_dir(&root, e2).join(lb::EVENTS_FILE)).unwrap()
        );
        assert!(lb::read_metadata(&lb::execution_dir(&root, e1))
            .unwrap()
            .pushed_at
            .is_some());
        // Second --all sends nothing; an explicit id is skipped without --force.
        run_push(&Client::new(), "http://unused", args(true, vec![]))
            .await
            .unwrap();
        run_push(&Client::new(), "http://unused", args(false, vec![e1]))
            .await
            .unwrap();
        assert_eq!(server.received_requests().await.unwrap().len(), 2);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn push_failure_stops_and_leaves_unpushed() {
        use wiremock::{matchers::method, Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(501))
            .mount(&server)
            .await;
        let root = std::env::temp_dir().join(format!("noetl-push3-{}", lb::new_id()));
        let e1 = closed_buffer(&root, "s", 1);
        let _e2 = closed_buffer(&root, "s", 1);
        let args = PushArgs {
            execution_ids: vec![],
            all: true,
            stack: None,
            force: false,
            dry_run: false,
            server: Some(server.uri()),
            root: BufferRoot {
                dir: Some(root.clone()),
            },
        };
        let err = run_push(&Client::new(), "http://unused", args)
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("501"), "{err}");
        assert_eq!(
            server.received_requests().await.unwrap().len(),
            1,
            "stops at the first failure"
        );
        assert!(lb::read_metadata(&lb::execution_dir(&root, e1))
            .unwrap()
            .pushed_at
            .is_none());
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn labels_parse() {
        let m = parse_labels(&["a=b".into(), "c=d=e".into()]).unwrap();
        assert_eq!(m["c"], "d=e");
        assert!(parse_labels(&["nope".into()]).is_err());
    }
}

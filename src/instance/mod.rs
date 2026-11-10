//! `noetl instance`: give this NoETL an identity, register it with a control plane,
//! discover peers, hand work to them, and take work from them (noetl/ai-meta#544 item 9).
//!
//! Everything here is **outbound**. Registration is a signed call on a timer; taking work is
//! a long-poll this process opens; results go back on another outbound call. So an instance
//! behind NAT or a firewall that only allows egress (a laptop, an edge box) takes part fully.
//!
//! The server side is noetl/server `src/instances/` (`NOETL_INSTANCE_REGISTRY=true`).

// The mirror keeps the server's verify path so its tests can check what this side signs.
#[allow(dead_code)]
pub mod identity;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use base64::Engine;
use clap::{Args, Subcommand};
use rand::RngCore;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use identity::InstanceKey;

/// Tool kinds the local runtime executes, advertised by default. Guarded by a test that
/// parses each one through the executor's `Tool` enum, so this list cannot claim a kind
/// the runtime would reject.
pub const LOCAL_TOOL_KINDS: [&str; 9] = [
    "shell", "http", "playbook", "duckdb", "auth", "sink", "rhai", "provider", "agent",
];

#[derive(Subcommand, Debug)]
pub enum InstanceCommand {
    /// Create this instance's ML-DSA-87 key (once) and print its id
    Init {
        #[command(flatten)]
        key: KeyArg,
        /// Replace an existing key. The instance gets a NEW id.
        #[arg(long)]
        force: bool,
    },
    /// Print this instance's id
    Id {
        #[command(flatten)]
        key: KeyArg,
    },
    /// Register with a control plane and keep the registration alive
    Register {
        #[command(flatten)]
        conn: Conn,
        #[command(flatten)]
        caps: CapsArgs,
        /// Register once and exit instead of heartbeating
        #[arg(long)]
        once: bool,
    },
    /// List registered instances that match the filters
    Discover {
        #[command(flatten)]
        conn: Conn,
        #[command(flatten)]
        filter: FilterArgs,
    },
    /// Ask a peer to run a playbook; print the task, or its result with --wait
    Handoff {
        #[command(flatten)]
        conn: Conn,
        /// Target instance id. Without it, the freshest live match for the filters is used.
        #[arg(long)]
        to: Option<String>,
        #[command(flatten)]
        filter: FilterArgs,
        /// Playbook path the target runs (its own copy)
        #[arg(long)]
        playbook: String,
        /// Pin the content: the target refuses unless its copy has this SHA-256. Give a
        /// local file to hash, or the hex digest.
        #[arg(long, value_name = "FILE|HEX")]
        pin: Option<String>,
        /// Payload, a JSON object; becomes the target run's variables
        #[arg(long, default_value = "{}")]
        payload: String,
        /// Wait up to this many seconds for the result (polling, outbound)
        #[arg(long, default_value_t = 0)]
        wait: u64,
        /// Ask the server to POST the result here when done (origin must be allowed there)
        #[arg(long)]
        callback_url: Option<String>,
    },
    /// Register, heartbeat, and run tasks other instances hand to this one
    Serve {
        #[command(flatten)]
        conn: Conn,
        #[command(flatten)]
        caps: CapsArgs,
        /// Directory the playbook paths resolve under (`<dir>/<path>.yaml`)
        #[arg(long, value_name = "DIR")]
        playbook_dir: PathBuf,
        /// Exit after handling this many tasks (0 = run until stopped)
        #[arg(long, default_value_t = 0)]
        max_tasks: u64,
        /// Also accept tasks pushed by the control plane: listen here (e.g. 0.0.0.0:7410).
        /// Needs --push-endpoint. The inbox is still polled, so nothing depends on the push.
        #[arg(long, value_name = "ADDR", requires = "push_endpoint")]
        push_listen: Option<String>,
        /// The URL the control plane should push to (what reaches --push-listen). Its origin
        /// must be in the control plane's NOETL_INSTANCE_PUSH_ORIGINS.
        #[arg(long, value_name = "URL", requires = "push_listen")]
        push_endpoint: Option<String>,
    },
    /// Become trusted by a control plane in enrollment mode, with a token an operator minted
    Enroll {
        #[command(flatten)]
        conn: Conn,
        /// The enrollment token (`nie-…`). Prefer --token-file to keep it out of shell history.
        #[arg(long, conflicts_with = "token_file")]
        token: Option<String>,
        /// A file holding the enrollment token
        #[arg(long, value_name = "FILE")]
        token_file: Option<PathBuf>,
    },
}

#[derive(Args, Debug, Clone)]
pub struct KeyArg {
    /// Key file (default: ~/.noetl/instance/key.json)
    #[arg(long = "key", value_name = "FILE")]
    pub path: Option<PathBuf>,
}

#[derive(Args, Debug, Clone)]
pub struct Conn {
    /// Control plane URL (a noetl-server with NOETL_INSTANCE_REGISTRY=true)
    #[arg(long)]
    pub server: String,
    /// Audience the control plane expects (its NOETL_INSTANCE_AUDIENCE)
    #[arg(long, default_value = "noetl")]
    pub audience: String,
    #[command(flatten)]
    pub key: KeyArg,
}

#[derive(Args, Debug, Clone)]
pub struct CapsArgs {
    /// How this instance runs NoETL: cli | daemon | distributed
    #[arg(long = "runtime-kind", default_value = "cli")]
    pub runtime_kind: String,
    #[arg(long)]
    pub name: Option<String>,
    #[arg(long)]
    pub site: Option<String>,
    /// key=value, repeatable
    #[arg(long = "label", value_name = "K=V")]
    pub labels: Vec<String>,
    /// Tool kinds to advertise (default: the local runtime's kinds)
    #[arg(long = "tool-kind", value_name = "KIND")]
    pub tool_kinds: Vec<String>,
    /// Worker pool, repeatable (a distributed instance)
    #[arg(long = "pool")]
    pub pools: Vec<String>,
    /// Playbook path or `*`-glob this instance runs for others, repeatable
    #[arg(long = "offer", value_name = "PATH|GLOB")]
    pub offer: Vec<String>,
    /// Take tasks from other instances (implied by `serve`)
    #[arg(long)]
    pub accept_tasks: bool,
    /// A peer instance id this one knows, repeatable
    #[arg(long = "peer", value_name = "ID")]
    pub peers: Vec<String>,
    /// How work reaches this instance: inbox (pull, the default) | push (also POSTed to
    /// --endpoint when the control plane allows it). `serve --push-endpoint` sets both.
    #[arg(long, value_name = "inbox|push")]
    pub reach: Option<String>,
    /// The URL a push goes to (a receiver this instance, or a sidecar, serves)
    #[arg(long, value_name = "URL")]
    pub endpoint: Option<String>,
}

#[derive(Args, Debug, Clone, Default)]
pub struct FilterArgs {
    #[arg(long = "with-tool-kind", value_name = "KIND")]
    pub tool_kind: Vec<String>,
    #[arg(long = "in-pool")]
    pub pool: Option<String>,
    #[arg(long = "at-site")]
    pub site: Option<String>,
    #[arg(long = "with-label", value_name = "K=V")]
    pub label: Vec<String>,
    /// Instances willing to run this playbook path
    #[arg(long = "runs")]
    pub runs: Option<String>,
    #[arg(long = "of-kind")]
    pub runtime_kind: Option<String>,
    #[arg(long)]
    pub include_stale: bool,
}

impl FilterArgs {
    fn query(&self) -> Value {
        let mut q = json!({});
        if !self.tool_kind.is_empty() {
            q["tool_kind"] = json!(self.tool_kind);
        }
        if !self.label.is_empty() {
            q["label"] = json!(self.label);
        }
        for (k, v) in [
            ("pool", &self.pool),
            ("site", &self.site),
            ("playbook", &self.runs),
            ("runtime_kind", &self.runtime_kind),
        ] {
            if let Some(v) = v {
                q[k] = json!(v);
            }
        }
        if self.include_stale {
            q["include_stale"] = json!(true);
        }
        q
    }
}

impl CapsArgs {
    pub fn body(&self) -> Result<Value> {
        if !matches!(self.runtime_kind.as_str(), "cli" | "daemon" | "distributed") {
            bail!("--runtime-kind must be cli, daemon or distributed");
        }
        let mut labels = BTreeMap::new();
        for kv in &self.labels {
            let (k, v) = kv.split_once('=').ok_or_else(|| anyhow!("--label `{kv}` is not K=V"))?;
            labels.insert(k.to_string(), v.to_string());
        }
        let tool_kinds: Vec<String> = if self.tool_kinds.is_empty() {
            LOCAL_TOOL_KINDS.iter().map(|s| s.to_string()).collect()
        } else {
            self.tool_kinds.clone()
        };
        let mut b = json!({
            "runtime_kind": self.runtime_kind,
            "version": format!("noetl-cli/{}", env!("CARGO_PKG_VERSION")),
            "labels": labels,
            "tool_kinds": tool_kinds,
            "pools": self.pools,
            "playbooks": self.offer,
            "accepts_tasks": self.accept_tasks,
            "known_peers": self.peers,
        });
        if let Some(r) = &self.reach {
            if !matches!(r.as_str(), "inbox" | "push") {
                bail!("--reach must be inbox or push");
            }
        }
        for (k, v) in [
            ("name", &self.name),
            ("site", &self.site),
            ("reach", &self.reach),
            ("endpoint", &self.endpoint),
        ] {
            if let Some(v) = v {
                b[k] = json!(v);
            }
        }
        Ok(b)
    }
}

pub fn default_key_path() -> Result<PathBuf> {
    let home = dirs::home_dir().ok_or_else(|| anyhow!("no home directory; pass --key"))?;
    Ok(home.join(".noetl").join("instance").join("key.json"))
}

fn key_path(k: &KeyArg) -> Result<PathBuf> {
    k.path.clone().map(Ok).unwrap_or_else(default_key_path)
}

const B64: base64::engine::GeneralPurpose = base64::engine::general_purpose::STANDARD;

/// Write a new key. The file holds the seed, so it is created 0600 in a 0700 directory and
/// never overwritten unless `force`.
pub fn init_key(path: &Path, force: bool) -> Result<InstanceKey> {
    if path.exists() && !force {
        bail!(
            "{} already exists; it IS this instance's identity. Use --force to replace it (new id).",
            path.display()
        );
    }
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))?;
        }
    }
    let key = InstanceKey::generate();
    let doc = json!({"alg": identity::ALG, "instance_id": key.id(), "seed": B64.encode(key.seed())});
    let tmp = path.with_extension("tmp");
    {
        let mut opts = std::fs::OpenOptions::new();
        opts.write(true).create(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(0o600);
        }
        let mut f = opts.open(&tmp).with_context(|| format!("creating {}", tmp.display()))?;
        std::io::Write::write_all(&mut f, serde_json::to_string_pretty(&doc)?.as_bytes())?;
    }
    std::fs::rename(&tmp, path)?;
    Ok(key)
}

pub fn load_key(path: &Path) -> Result<InstanceKey> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("reading {} (run `noetl instance init` first)", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(path)?.permissions().mode() & 0o777;
        if mode & 0o077 != 0 {
            bail!("{} is readable by others (mode {mode:o}); chmod 600 it", path.display());
        }
    }
    let doc: Value = serde_json::from_str(&text).context("key file is not JSON")?;
    if doc["alg"] != identity::ALG {
        bail!("key file alg is {}, expected {}", doc["alg"], identity::ALG);
    }
    let seed = B64
        .decode(doc["seed"].as_str().unwrap_or_default())
        .context("key file seed")?;
    let seed: [u8; 32] = seed.try_into().map_err(|_| anyhow!("key file seed is not 32 bytes"))?;
    let key = InstanceKey::from_seed(seed);
    if doc["instance_id"].as_str().is_some_and(|id| id != key.id()) {
        bail!("key file instance_id does not match its seed; the file was edited");
    }
    Ok(key)
}

fn nonce() -> String {
    let mut b = [0u8; 16];
    rand::rngs::OsRng.fill_bytes(&mut b);
    identity::to_hex(&b)
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// The text an instance signs. Mirrors noetl/server `instances::gate::payload`.
pub fn payload(purpose: &str, instance_id: &str, audience: &str, ts_ms: i64, nonce: &str, body: Value) -> String {
    json!({
        "v": 1,
        "purpose": purpose,
        "instance_id": instance_id,
        "audience": audience,
        "ts_ms": ts_ms,
        "nonce": nonce,
        "body": body,
    })
    .to_string()
}

/// A signed-request client for one control plane.
pub struct Client {
    http: reqwest::Client,
    base: String,
    audience: String,
    key: InstanceKey,
}

impl Client {
    pub fn new(conn: &Conn) -> Result<Self> {
        let key = load_key(&key_path(&conn.key)?)?;
        Ok(Client::with_key(&conn.server, &conn.audience, key))
    }

    pub fn with_key(server: &str, audience: &str, key: InstanceKey) -> Self {
        Client {
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(60))
                .build()
                .expect("http client"),
            base: server.trim_end_matches('/').to_string(),
            audience: audience.to_string(),
            key,
        }
    }

    pub fn id(&self) -> &str {
        self.key.id()
    }

    /// POST a signed request; `Ok((status, body))` for any HTTP answer.
    pub async fn call(&self, route: &str, purpose: &str, body: Value) -> Result<(u16, Value)> {
        let env = self.key.sign(payload(
            purpose,
            self.key.id(),
            &self.audience,
            now_ms(),
            &nonce(),
            body,
        ));
        let r = self
            .http
            .post(format!("{}{route}", self.base))
            .json(&env)
            .send()
            .await
            .with_context(|| format!("POST {}{route}", self.base))?;
        let status = r.status().as_u16();
        Ok((status, r.json().await.unwrap_or(Value::Null)))
    }

    /// Like [`Client::call`], but a non-2xx answer is an error carrying the server's reason.
    pub async fn ok(&self, route: &str, purpose: &str, body: Value) -> Result<Value> {
        let (status, v) = self.call(route, purpose, body).await?;
        if !(200..300).contains(&status) {
            bail!(
                "{route}: HTTP {status}: {}",
                v["error"].as_str().unwrap_or(&v.to_string())
            );
        }
        Ok(v)
    }

    pub async fn register(&self, caps: &Value) -> Result<Value> {
        self.ok("/api/instances/register", "register", caps.clone()).await
    }
}

/// Resolve a task's playbook path to a file under `dir`, refusing anything that escapes it.
pub fn resolve_playbook(dir: &Path, path: &str) -> Result<PathBuf> {
    if path.is_empty() || path.contains("..") || path.starts_with('/') || path.contains('\\') {
        bail!("playbook path `{path}` is not a relative path under the playbook dir");
    }
    let root = dir
        .canonicalize()
        .with_context(|| format!("playbook dir {}", dir.display()))?;
    let candidates = if path.ends_with(".yaml") || path.ends_with(".yml") {
        vec![root.join(path)]
    } else {
        vec![root.join(format!("{path}.yaml")), root.join(format!("{path}.yml"))]
    };
    for c in candidates {
        if let Ok(real) = c.canonicalize() {
            if !real.starts_with(&root) {
                bail!("playbook `{path}` resolves outside the playbook dir");
            }
            return Ok(real);
        }
    }
    bail!("no playbook `{path}` under {}", root.display())
}

fn glob_match(pattern: &str, path: &str) -> bool {
    let parts: Vec<&str> = pattern.split('*').collect();
    if parts.len() == 1 {
        return pattern == path;
    }
    let (first, last) = (parts[0], parts[parts.len() - 1]);
    if !path.starts_with(first) || path.len() < first.len() + last.len() || !path.ends_with(last) {
        return false;
    }
    let mut rest = &path[first.len()..path.len() - last.len()];
    for mid in &parts[1..parts.len() - 1] {
        match rest.find(mid) {
            Some(i) => rest = &rest[i + mid.len()..],
            None => return false,
        }
    }
    true
}

/// Run one claimed task locally. `Ok(result)` → completed; `Err(reason)` → failed.
pub fn run_task(task: &Value, playbook_dir: &Path, offer: &[String]) -> std::result::Result<Value, String> {
    let path = task["playbook"]["path"].as_str().unwrap_or_default();
    // The server checked this against what we advertised; check again, because the scope is
    // ours to enforce.
    if !offer.iter().any(|g| glob_match(g, path)) {
        return Err(format!("this instance does not offer `{path}`"));
    }
    let file = resolve_playbook(playbook_dir, path).map_err(|e| e.to_string())?;
    if let Some(want) = task["playbook"]["sha256"].as_str() {
        let text = std::fs::read(&file).map_err(|e| e.to_string())?;
        let have = identity::to_hex(&Sha256::digest(&text));
        if !have.eq_ignore_ascii_case(want) {
            return Err(format!(
                "playbook `{path}` here is sha256 {have}, the submitter pinned {want}"
            ));
        }
    }
    let payload = &task["payload"];
    if !(payload.is_object() || payload.is_null()) {
        return Err("payload must be a JSON object".into());
    }
    let payload_text = if payload.is_null() {
        "{}".to_string()
    } else {
        payload.to_string()
    };
    let vars = crate::build_variables(Some(&payload_text), &[]).map_err(|e| e.to_string())?;
    let runner = crate::PlaybookRunner::new(file).with_variables(vars).with_quiet(true);
    let outcome = runner.run().map_err(|e| format!("{e:#}"))?;
    let final_result = outcome
        .final_result
        .as_deref()
        .map(|s| serde_json::from_str::<Value>(s).unwrap_or_else(|_| json!(s)));
    let view = json!({
        "status": outcome.status,
        "final_result": final_result,
        "executed_steps": outcome.executed_steps,
        "duration_seconds": outcome.duration_seconds,
    });
    if outcome.status == "ok" {
        Ok(view)
    } else {
        Err(outcome
            .error
            .unwrap_or_else(|| format!("playbook ended with status {}", outcome.status)))
    }
}

/// Constant-time equality for the push bearer.
fn same(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// The push receiver: checks the bearer the control plane issued at registration and
/// forwards each pushed task to the run loop. Answers 202 at once; results go back signed.
async fn push_receiver(listener: tokio::net::TcpListener, token: String, tx: tokio::sync::mpsc::Sender<Value>) {
    use axum::http::{HeaderMap, StatusCode};
    let expected = format!("Bearer {token}");
    let app = axum::Router::new().fallback(axum::routing::post(
        move |headers: HeaderMap, body: axum::body::Bytes| {
            let (expected, tx) = (expected.clone(), tx.clone());
            async move {
                let auth = headers
                    .get("authorization")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or_default();
                if !same(auth.as_bytes(), expected.as_bytes()) {
                    return StatusCode::UNAUTHORIZED;
                }
                let Ok(v) = serde_json::from_slice::<Value>(&body) else {
                    return StatusCode::BAD_REQUEST;
                };
                for t in v["tasks"].as_array().cloned().unwrap_or_default() {
                    if tx.send(t).await.is_err() {
                        return StatusCode::SERVICE_UNAVAILABLE;
                    }
                }
                StatusCode::ACCEPTED
            }
        },
    ));
    if let Err(e) = axum::serve(listener, app).await {
        eprintln!("push receiver stopped: {e}");
    }
}

async fn heartbeat_forever(client: std::sync::Arc<Client>, caps: Value, mut every: u64) {
    loop {
        tokio::time::sleep(Duration::from_secs(every.max(1))).await;
        match client.register(&caps).await {
            Ok(r) => every = r["heartbeat_secs"].as_u64().unwrap_or(every),
            // A missed beat is not fatal: the lease is three beats long.
            Err(e) => eprintln!("instance heartbeat failed: {e:#}"),
        }
    }
}

pub async fn run(cmd: InstanceCommand) -> Result<()> {
    match cmd {
        InstanceCommand::Init { key, force } => {
            let path = key_path(&key)?;
            let k = init_key(&path, force)?;
            println!("{}", k.id());
            eprintln!(
                "key written to {} (keep it private; it is this instance's identity)",
                path.display()
            );
        }
        InstanceCommand::Id { key } => println!("{}", load_key(&key_path(&key)?)?.id()),
        InstanceCommand::Register { conn, caps, once } => {
            let client = Client::new(&conn)?;
            let body = caps.body()?;
            let r = client.register(&body).await?;
            println!("{}", serde_json::to_string_pretty(&r)?);
            if !once {
                let every = r["heartbeat_secs"].as_u64().unwrap_or(60);
                heartbeat_forever(std::sync::Arc::new(client), body, every).await;
            }
        }
        InstanceCommand::Discover { conn, filter } => {
            let client = Client::new(&conn)?;
            let r = client.ok("/api/instances/discover", "discover", filter.query()).await?;
            println!("{}", serde_json::to_string_pretty(&r)?);
        }
        InstanceCommand::Handoff {
            conn,
            to,
            filter,
            playbook,
            pin,
            payload,
            wait,
            callback_url,
        } => {
            let client = Client::new(&conn)?;
            let payload: Value = serde_json::from_str(&payload).context("--payload is not JSON")?;
            let target = match to {
                Some(id) => json!({"instance_id": id}),
                None => json!({"selector": filter.query()}),
            };
            let mut pb = json!({"path": playbook});
            if let Some(p) = pin {
                let hex = if p.len() == 64 && p.bytes().all(|b| b.is_ascii_hexdigit()) {
                    p
                } else {
                    identity::to_hex(&Sha256::digest(
                        std::fs::read(&p).with_context(|| format!("--pin {p}"))?,
                    ))
                };
                pb["sha256"] = json!(hex);
            }
            let mut body = json!({"target": target, "playbook": pb, "payload": payload});
            if let Some(u) = callback_url {
                body["callback_url"] = json!(u);
            }
            let sub = client.ok("/api/instances/tasks/submit", "task.submit", body).await?;
            if wait == 0 {
                println!("{}", serde_json::to_string_pretty(&sub)?);
                return Ok(());
            }
            let task_id = sub["task_id"].clone();
            let until = tokio::time::Instant::now() + Duration::from_secs(wait);
            loop {
                let v = client
                    .ok(
                        "/api/instances/tasks/status",
                        "task.status",
                        json!({"task_id": task_id}),
                    )
                    .await?;
                let state = v["state"].as_str().unwrap_or_default().to_string();
                if matches!(state.as_str(), "completed" | "failed" | "expired") || tokio::time::Instant::now() >= until
                {
                    println!("{}", serde_json::to_string_pretty(&v)?);
                    if state != "completed" {
                        bail!("task {task_id} ended {state}");
                    }
                    break;
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
        InstanceCommand::Enroll {
            conn,
            token,
            token_file,
        } => {
            let token = match (token, token_file) {
                (Some(t), _) => t,
                (None, Some(f)) => {
                    std::fs::read_to_string(&f).with_context(|| format!("--token-file {}", f.display()))?
                }
                (None, None) => bail!("give --token or --token-file"),
            };
            let client = Client::new(&conn)?;
            let r = client
                .ok("/api/instances/enroll", "enroll", json!({"token": token.trim()}))
                .await?;
            println!("{}", serde_json::to_string_pretty(&r)?);
        }
        InstanceCommand::Serve {
            conn,
            mut caps,
            playbook_dir,
            max_tasks,
            push_listen,
            push_endpoint,
        } => {
            caps.accept_tasks = true;
            if caps.offer.is_empty() {
                bail!("serve needs at least one --offer PATH|GLOB: what this instance runs for others");
            }
            let client = std::sync::Arc::new(Client::new(&conn)?);
            let mut body = caps.body()?;
            // Bind before registering, so a push that follows the registration has a listener.
            let listener = match &push_listen {
                Some(addr) => Some(
                    tokio::net::TcpListener::bind(addr)
                        .await
                        .with_context(|| format!("--push-listen {addr}"))?,
                ),
                None => None,
            };
            if let Some(ep) = &push_endpoint {
                body["reach"] = json!("push");
                body["endpoint"] = json!(ep);
            }
            let r = client.register(&body).await?;
            eprintln!(
                "instance {} registered with {} as {}",
                client.id(),
                conn.server,
                r["control_plane"]
            );
            let (tx, mut rx) = tokio::sync::mpsc::channel::<Value>(64);
            let mut push = None;
            if let Some(l) = listener {
                match r["push_token"].as_str() {
                    Some(tok) => {
                        eprintln!(
                            "accepting pushes on {} for {}",
                            push_listen.as_deref().unwrap_or(""),
                            push_endpoint.as_deref().unwrap_or("")
                        );
                        push = Some(tokio::spawn(push_receiver(l, tok.to_string(), tx.clone())));
                    }
                    None => eprintln!("the control plane issued no push token (push is off there); pulling only"),
                }
            }
            let every = r["heartbeat_secs"].as_u64().unwrap_or(60);
            let hb = tokio::spawn(heartbeat_forever(client.clone(), body, every));
            // The inbox is always polled: push is an accelerator, never the only path.
            let poller = {
                let (client, tx) = (client.clone(), tx.clone());
                tokio::spawn(async move {
                    loop {
                        match client
                            .ok("/api/instances/inbox", "inbox.claim", json!({"wait_ms": 20_000}))
                            .await
                        {
                            Ok(v) => {
                                for t in v["tasks"].as_array().cloned().unwrap_or_default() {
                                    if tx.send(t).await.is_err() {
                                        return;
                                    }
                                }
                            }
                            Err(e) => {
                                eprintln!("inbox poll failed: {e:#}; retrying");
                                tokio::time::sleep(Duration::from_secs(5)).await;
                            }
                        }
                    }
                })
            };
            drop(tx);
            let mut handled = 0u64;
            let result: Result<()> = loop {
                if max_tasks > 0 && handled >= max_tasks {
                    break Ok(());
                }
                let Some(task) = rx.recv().await else {
                    break Ok(());
                };
                {
                    let task_id = task["task_id"].clone();
                    eprintln!("task {task_id} from {}: {}", task["from"], task["playbook"]["path"]);
                    let (dir, offer, t) = (playbook_dir.clone(), caps.offer.clone(), task.clone());
                    let outcome = tokio::task::block_in_place(|| run_task(&t, &dir, &offer));
                    let mut body = json!({"task_id": task_id, "claim_id": task["claim_id"]});
                    match outcome {
                        Ok(v) => {
                            body["state"] = json!("completed");
                            body["result"] = v;
                        }
                        Err(e) => {
                            body["state"] = json!("failed");
                            body["error"] = json!(e);
                        }
                    }
                    match client.call("/api/instances/tasks/result", "task.result", body).await {
                        Ok((s, v)) if (200..300).contains(&s) => eprintln!("task {task_id} reported: {v}"),
                        Ok((s, v)) => eprintln!("task {task_id} result refused: HTTP {s} {v}"),
                        Err(e) => eprintln!("task {task_id} result not delivered: {e:#}"),
                    }
                    handled += 1;
                }
            };
            poller.abort();
            if let Some(p) = push {
                p.abort();
            }
            hb.abort();
            let _ = client.call("/api/instances/deregister", "deregister", json!({})).await;
            return result;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_advertised_kind_is_one_the_local_runtime_knows() {
        for kind in LOCAL_TOOL_KINDS {
            let parsed: std::result::Result<noetl_executor::playbook::Tool, _> =
                serde_yaml::from_str(&format!("kind: {kind}"));
            // A known kind with missing fields is a parse error; an unknown one is Unsupported.
            assert!(
                !matches!(parsed, Ok(noetl_executor::playbook::Tool::Unsupported)),
                "`{kind}` is advertised but the local runtime does not know it"
            );
        }
        let mcp: noetl_executor::playbook::Tool = serde_yaml::from_str("kind: mcp").unwrap();
        assert!(
            matches!(mcp, noetl_executor::playbook::Tool::Unsupported),
            "control: an unknown kind is Unsupported"
        );
    }

    #[tokio::test]
    async fn the_push_receiver_checks_the_bearer_and_forwards_tasks() {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/push", l.local_addr().unwrap());
        let (tx, mut rx) = tokio::sync::mpsc::channel(8);
        tokio::spawn(push_receiver(l, "npt-secret".into(), tx));
        let http = reqwest::Client::new();
        let body = json!({"tasks": [{"task_id": "rt-1", "claim_id": "claim-1"}]});
        let r = http
            .post(&url)
            .bearer_auth("npt-wrong")
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(r.status().as_u16(), 401);
        let r = http.post(&url).json(&body).send().await.unwrap();
        assert_eq!(r.status().as_u16(), 401, "no bearer");
        let r = http
            .post(&url)
            .bearer_auth("npt-secret")
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(r.status().as_u16(), 202);
        let t = rx.recv().await.unwrap();
        assert_eq!(t["task_id"], "rt-1");
        assert!(rx.try_recv().is_err(), "the refused pushes forwarded nothing");
    }

    #[test]
    fn bearer_comparison() {
        assert!(same(b"Bearer a", b"Bearer a"));
        assert!(!same(b"Bearer a", b"Bearer b"));
        assert!(!same(b"Bearer a", b"Bearer ab"));
    }

    #[test]
    fn the_key_file_is_private_and_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("inst").join("key.json");
        let k = init_key(&path, false).unwrap();
        assert_eq!(load_key(&path).unwrap().id(), k.id());
        assert!(init_key(&path, false).is_err(), "never overwritten without --force");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(std::fs::metadata(&path).unwrap().permissions().mode() & 0o777, 0o600);
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
            assert!(load_key(&path).is_err(), "a world-readable key is refused");
        }
    }

    #[test]
    fn task_paths_cannot_escape_the_playbook_dir() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("edge")).unwrap();
        std::fs::write(dir.path().join("edge/probe.yaml"), "x").unwrap();
        assert!(resolve_playbook(dir.path(), "edge/probe").is_ok());
        for bad in ["../x", "/etc/passwd", "edge/../../x", "edge\\probe", ""] {
            assert!(resolve_playbook(dir.path(), bad).is_err(), "{bad}");
        }
        #[cfg(unix)]
        {
            let outside = tempfile::tempdir().unwrap();
            std::fs::write(outside.path().join("evil.yaml"), "x").unwrap();
            std::os::unix::fs::symlink(outside.path().join("evil.yaml"), dir.path().join("link.yaml")).unwrap();
            assert!(
                resolve_playbook(dir.path(), "link").is_err(),
                "a symlink out is refused"
            );
        }
    }

    #[test]
    fn caps_default_to_the_local_kinds_and_reject_bad_labels() {
        let caps = CapsArgs {
            runtime_kind: "daemon".into(),
            name: None,
            site: Some("home".into()),
            labels: vec!["arch=arm64".into()],
            tool_kinds: vec![],
            pools: vec![],
            offer: vec!["edge/*".into()],
            accept_tasks: true,
            peers: vec![],
        };
        let b = caps.body().unwrap();
        assert_eq!(b["tool_kinds"].as_array().unwrap().len(), LOCAL_TOOL_KINDS.len());
        assert_eq!(b["labels"]["arch"], "arm64");
        assert!(b["version"].as_str().unwrap().starts_with("noetl-cli/"));
        let mut bad = caps.clone();
        bad.labels = vec!["nope".into()];
        assert!(bad.body().is_err());
        let mut bad = caps;
        bad.runtime_kind = "server".into();
        assert!(bad.body().is_err());
    }

    pub(crate) const HELLO: &str = r#"
apiVersion: noetl.io/v2
kind: Playbook
metadata: { name: hello }
workload: { who: nobody }
workflow:
  - step: start
    next: [ { step: greet } ]
  - step: greet
    tool: { kind: shell, cmds: [ 'echo "hello {{ workload.who }}"' ] }
"#;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_task_runs_the_local_copy_with_its_payload_and_honours_the_pin() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("edge")).unwrap();
        std::fs::write(dir.path().join("edge/hello.yaml"), HELLO).unwrap();
        let offer = vec!["edge/*".to_string()];
        let task = json!({"playbook": {"path": "edge/hello"}, "payload": {"who": "peer"}});
        let v = run_task(&task, dir.path(), &offer).unwrap();
        assert_eq!(v["status"], "ok");
        assert!(v["final_result"].to_string().contains("hello peer"), "{v}");

        let pinned = identity::to_hex(&Sha256::digest(HELLO.as_bytes()));
        let task = json!({"playbook": {"path": "edge/hello", "sha256": pinned}, "payload": {}});
        assert!(run_task(&task, dir.path(), &offer).is_ok());
        let task = json!({"playbook": {"path": "edge/hello", "sha256": "0".repeat(64)}, "payload": {}});
        assert!(run_task(&task, dir.path(), &offer).unwrap_err().contains("pinned"));
        let task = json!({"playbook": {"path": "core/hello"}, "payload": {}});
        assert!(run_task(&task, dir.path(), &offer)
            .unwrap_err()
            .contains("does not offer"));
    }
}

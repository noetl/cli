//! `noetl serve` — playbooks as an MCP server and as agents, on the local
//! runtime (and, through `noetl subscribe --serve`, on the daemon).
//!
//! The same surface noetl-server exposes behind `NOETL_MCP_SERVER` and
//! `NOETL_AGENT_ENDPOINT` (build plan items 5 and 7), answered by running the
//! playbook in-process with [`PlaybookRunner`]:
//!
//! | Route | What |
//! |---|---|
//! | `POST /mcp` | MCP Streamable HTTP, JSON replies, stateless: `initialize`, `ping`, `tools/list`, `tools/call`; notifications get `202` |
//! | `GET /api/agents` | the cards of every playbook with `metadata.expose.agent` |
//! | `GET /api/agents/cards/{*path}` | one card |
//! | `POST /api/agents/send` | `{agent, input, callback_url?}` → `202 {task_id, status, callback_token?}` |
//! | `GET /api/agents/tasks/{task_id}` | `{task_id, agent, status, result \| error}` |
//! | `GET /healthz` | liveness |
//!
//! # Same opt-in as the server
//!
//! A playbook is served only if it declares `metadata.expose.mcp` (`tool`,
//! `description`, `input_schema`, optional `mode`, `timeout`, `result_step`)
//! or `metadata.expose.agent` (`card.{name, description, version?, skills?}`,
//! `input_schema` or the mcp one, `result_step`). The input schema is written
//! by hand and is the argument allowlist: an argument not under `properties`
//! is refused unless `additionalProperties: true`, so a caller cannot set a
//! `workload` key the author did not expose. Arguments become `workload.*`
//! variables verbatim, never template-rendered, with `{{` defanged — the same
//! treatment model output gets in an agent's sub-playbook call.
//!
//! The playbook directory is rescanned on every listing and call, so editing a
//! playbook needs no restart. Files that declare `expose:` but cannot be
//! served are reported under `_meta["noetl/not_served"]` / `not_served`.
//!
//! # Exposure
//!
//! The listener binds `127.0.0.1` by default. `--token-file` requires
//! `Authorization: Bearer <token>` on every route except `/healthz`. Callbacks
//! go only to origins listed with `--callback-origin` (none → every
//! `callback_url` is refused), redirects are not followed, and the callback
//! carries `Authorization: Bearer <callback_token>`, a random token minted for
//! that task alone. Results pass through a key-based redaction of credential
//! fields before they leave the process.
//!
//! # What this is not
//!
//! Tasks live in memory: they are lost on restart, and the oldest finished
//! tasks are dropped past [`MAX_TASKS`]. At most `--max-concurrent` playbooks
//! run at once; more are refused with 503 rather than queued without bound.

use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result};
use axum::body::Bytes;
use axum::extract::{Path as AxPath, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::{json, Map, Value};
use tokio::sync::Semaphore;
use tracing::Instrument;

use crate::playbook_runner::{defang_template_markers, PlaybookRunner, RunOutcome};

/// Protocol revisions answered, newest first (same set as noetl-server).
pub const SUPPORTED_PROTOCOL_VERSIONS: [&str; 3] = ["2025-06-18", "2025-03-26", "2024-11-05"];
pub const HELPER_PREFIX: &str = "noetl.";
pub const HELPER_STATUS: &str = "noetl.execution.status";
pub const HELPER_RESULT: &str = "noetl.execution.result";
pub const DEFAULT_SYNC_TIMEOUT_SECS: u64 = 30;
pub const MAX_SYNC_TIMEOUT_SECS: u64 = 120;
/// Tasks kept in memory; the oldest finished ones are dropped first.
pub const MAX_TASKS: usize = 1024;
const MAX_SCAN_FILES: usize = 2000;
const MAX_BODY_BYTES: usize = 1024 * 1024;

const PARSE_ERROR: i64 = -32700;
const INVALID_REQUEST: i64 = -32600;
const METHOD_NOT_FOUND: i64 = -32601;
const INVALID_PARAMS: i64 = -32602;

/// `noetl serve` options.
#[derive(Debug, Clone)]
pub struct ServeConfig {
    pub playbooks: PathBuf,
    pub listen: SocketAddr,
    pub mcp: bool,
    pub agents: bool,
    /// Bearer token required on every route but `/healthz`.
    pub token: Option<String>,
    /// Allowed callback origins (`scheme://host[:port]`).
    pub callback_origins: Vec<String>,
    pub max_concurrent: usize,
    pub verbose: bool,
}

// ---------------------------------------------------------------------------
// Exposed playbooks
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Sync,
    Async,
}

#[derive(Debug, Clone)]
pub struct McpExpose {
    pub tool: String,
    pub description: String,
    pub input_schema: Value,
    pub mode: Mode,
    pub timeout: u64,
    pub result_step: Option<String>,
}

#[derive(Debug, Clone)]
pub struct AgentExpose {
    pub name: String,
    pub description: String,
    pub version: Option<String>,
    pub skills: Value,
    pub input_schema: Value,
    pub result_step: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Exposed {
    /// `metadata.path`, else the file path relative to the playbook dir
    /// without the extension.
    pub path: String,
    pub file: PathBuf,
    pub mcp: Option<McpExpose>,
    pub agent: Option<AgentExpose>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Problem {
    pub source: String,
    pub reason: String,
}

#[derive(Debug, Default, Clone)]
pub struct Catalog {
    pub playbooks: Vec<Exposed>,
    pub problems: Vec<Problem>,
}

impl Catalog {
    fn mcp_tool(&self, name: &str) -> Option<&Exposed> {
        self.playbooks
            .iter()
            .find(|p| p.mcp.as_ref().is_some_and(|m| m.tool == name))
    }

    fn agent(&self, path: &str) -> Option<&Exposed> {
        self.playbooks
            .iter()
            .find(|p| p.agent.is_some() && p.path == path.trim_matches('/'))
    }
}

fn valid_tool_name(name: &str) -> bool {
    (1..=64).contains(&name.len())
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-'))
}

fn text(v: Option<&serde_yaml::Value>) -> Option<String> {
    v.and_then(|x| x.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

fn to_json(v: &serde_yaml::Value) -> Value {
    serde_json::to_value(v).unwrap_or(Value::Null)
}

/// Parse one playbook's `metadata.expose`. `Ok(None)`: nothing exposed.
pub fn parse_exposed(path: &str, file: &Path, doc: &serde_yaml::Value) -> Result<Option<Exposed>, String> {
    let Some(expose) = doc.get("metadata").and_then(|m| m.get("expose")) else {
        return Ok(None);
    };
    let mcp_schema = expose.get("mcp").and_then(|m| m.get("input_schema"));
    let mcp = match expose.get("mcp") {
        None => None,
        Some(m) => {
            let tool = text(m.get("tool")).ok_or("`expose.mcp.tool` is required")?;
            if !valid_tool_name(&tool) {
                return Err(format!("`expose.mcp.tool` `{tool}` must match [A-Za-z0-9_.-]{{1,64}}"));
            }
            if tool.starts_with(HELPER_PREFIX) {
                return Err(format!("`expose.mcp.tool` may not start with `{HELPER_PREFIX}`"));
            }
            let description = text(m.get("description")).ok_or("`expose.mcp.description` is required")?;
            let input_schema = mcp_schema
                .map(to_json)
                .filter(Value::is_object)
                .ok_or("`expose.mcp.input_schema` (an object) is required and is written by hand")?;
            let mode = match text(m.get("mode")).as_deref() {
                None | Some("sync") => Mode::Sync,
                Some("async") => Mode::Async,
                Some(other) => return Err(format!("`expose.mcp.mode` must be sync or async, got `{other}`")),
            };
            let timeout = m
                .get("timeout")
                .and_then(|t| t.as_u64())
                .unwrap_or(DEFAULT_SYNC_TIMEOUT_SECS)
                .clamp(1, MAX_SYNC_TIMEOUT_SECS);
            Some(McpExpose {
                tool,
                description,
                input_schema,
                mode,
                timeout,
                result_step: text(m.get("result_step")),
            })
        }
    };
    let agent = match expose.get("agent") {
        None => None,
        Some(a) => {
            let card = a.get("card").ok_or("`expose.agent.card` (a mapping) is required")?;
            let name = text(card.get("name")).ok_or("`expose.agent.card.name` is required")?;
            let description = text(card.get("description")).ok_or("`expose.agent.card.description` is required")?;
            let input_schema = a
                .get("input_schema")
                .or(mcp_schema)
                .map(to_json)
                .filter(Value::is_object)
                .ok_or("`expose.agent.input_schema` is required and is written by hand (it is not derived from `workload`)")?;
            let skills = match card.get("skills") {
                None => json!([]),
                Some(s) if s.is_sequence() => to_json(s),
                Some(_) => return Err("`expose.agent.card.skills` must be a list".into()),
            };
            Some(AgentExpose {
                name,
                description,
                version: card.get("version").map(|v| match v {
                    serde_yaml::Value::String(s) => s.clone(),
                    other => to_json(other).to_string(),
                }),
                skills,
                input_schema,
                result_step: text(a.get("result_step")),
            })
        }
    };
    if mcp.is_none() && agent.is_none() {
        return Ok(None);
    }
    Ok(Some(Exposed {
        path: path.to_string(),
        file: file.to_path_buf(),
        mcp,
        agent,
    }))
}

/// Walk `dir` for `*.yaml` / `*.yml` playbooks that opt in.
pub fn scan(dir: &Path) -> Catalog {
    let mut files = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        let Ok(rd) = std::fs::read_dir(&d) else { continue };
        for e in rd.flatten() {
            let p = e.path();
            let name = e.file_name().to_string_lossy().to_string();
            if name.starts_with('.') || name == "target" || name == "node_modules" {
                continue;
            }
            if p.is_dir() {
                stack.push(p);
            } else if matches!(p.extension().and_then(|x| x.to_str()), Some("yaml" | "yml")) {
                files.push(p);
            }
            if files.len() >= MAX_SCAN_FILES {
                break;
            }
        }
    }
    files.sort();
    let mut cat = Catalog::default();
    for file in files {
        let Ok(body) = std::fs::read_to_string(&file) else {
            continue;
        };
        if !body.contains("expose") {
            continue;
        }
        let Ok(doc) = serde_yaml::from_str::<serde_yaml::Value>(&body) else {
            continue;
        };
        if doc.get("kind").and_then(|k| k.as_str()) != Some("Playbook") {
            continue;
        }
        let rel = file
            .strip_prefix(dir)
            .unwrap_or(&file)
            .with_extension("")
            .to_string_lossy()
            .replace('\\', "/");
        let path = doc
            .get("metadata")
            .and_then(|m| m.get("path"))
            .and_then(|p| p.as_str())
            .map(|p| p.trim_matches('/').to_string())
            .unwrap_or(rel);
        match parse_exposed(&path, &file, &doc) {
            Ok(Some(e)) => {
                if let Some(m) = &e.mcp {
                    if cat.mcp_tool(&m.tool).is_some() {
                        cat.problems.push(Problem {
                            source: path.clone(),
                            reason: format!("mcp tool name `{}` is already served by another playbook", m.tool),
                        });
                        continue;
                    }
                }
                if e.agent.is_some() && cat.agent(&e.path).is_some() {
                    cat.problems.push(Problem {
                        source: path.clone(),
                        reason: "agent path is already served by another playbook".into(),
                    });
                    continue;
                }
                cat.playbooks.push(e);
            }
            Ok(None) => {}
            Err(reason) => cat.problems.push(Problem { source: path, reason }),
        }
    }
    cat
}

// ---------------------------------------------------------------------------
// Arguments and results (same rules as noetl-server's /api/mcp)
// ---------------------------------------------------------------------------

pub fn check_arguments(schema: &Value, args: Option<&Value>) -> Result<Map<String, Value>, String> {
    let args = match args {
        None | Some(Value::Null) => Map::new(),
        Some(Value::Object(m)) => m.clone(),
        Some(_) => return Err("`arguments` must be an object".into()),
    };
    let props = schema
        .get("properties")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let open = schema.get("additionalProperties").and_then(Value::as_bool) == Some(true);
    if let Some(required) = schema.get("required").and_then(Value::as_array) {
        let missing: Vec<&str> = required
            .iter()
            .filter_map(Value::as_str)
            .filter(|k| !args.contains_key(*k))
            .collect();
        if !missing.is_empty() {
            return Err(format!("missing required argument(s): {}", missing.join(", ")));
        }
    }
    for (key, value) in &args {
        match props.get(key) {
            None if open => {}
            None => return Err(format!("argument `{key}` is not declared in the tool's input_schema")),
            Some(prop) => {
                if let Some(ty) = prop.get("type").and_then(Value::as_str) {
                    let ok = match ty {
                        "string" => value.is_string(),
                        "integer" => value.is_i64() || value.is_u64(),
                        "number" => value.is_number(),
                        "boolean" => value.is_boolean(),
                        "object" => value.is_object(),
                        "array" => value.is_array(),
                        "null" => value.is_null(),
                        _ => true,
                    };
                    if !ok {
                        return Err(format!("argument `{key}` must be of type `{ty}`"));
                    }
                }
            }
        }
    }
    Ok(args)
}

/// Keys whose values are masked before a result leaves the process.
const SENSITIVE: &[&str] = &[
    "password",
    "passwd",
    "secret",
    "token",
    "api_key",
    "apikey",
    "authorization",
    "access_key",
    "private_key",
    "client_secret",
    "credential",
];

pub fn redact(v: &Value) -> Value {
    match v {
        Value::Object(m) => Value::Object(
            m.iter()
                .map(|(k, val)| {
                    let lk = k.to_ascii_lowercase();
                    if SENSITIVE.iter().any(|s| lk.contains(s)) && !val.is_object() && !val.is_array() {
                        (k.clone(), json!("***"))
                    } else {
                        (k.clone(), redact(val))
                    }
                })
                .collect(),
        ),
        Value::Array(a) => Value::Array(a.iter().map(redact).collect()),
        other => other.clone(),
    }
}

fn parse_value(s: &str) -> Value {
    serde_json::from_str(s).unwrap_or_else(|_| Value::String(s.to_string()))
}

/// The result a caller sees: `result_step`'s result, else the playbook's
/// final result (the same fallback `kind: playbook` `return_result` uses).
pub fn select_result(outcome: &RunOutcome, result_step: Option<&str>) -> Result<Value, String> {
    match result_step {
        Some(step) => outcome
            .step_results
            .get(step)
            .map(|s| parse_value(s))
            .ok_or_else(|| format!("result_step `{step}` produced no result")),
        None => Ok(outcome.final_result.as_deref().map(parse_value).unwrap_or(Value::Null)),
    }
}

// ---------------------------------------------------------------------------
// Runs and tasks
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
enum TaskState {
    Working,
    Completed(Value),
    Failed(String),
}

#[derive(Debug, Clone)]
struct Task {
    agent: String,
    state: TaskState,
}

#[derive(Default)]
struct Tasks {
    by_id: HashMap<String, Task>,
    order: VecDeque<String>,
}

impl Tasks {
    fn insert(&mut self, id: String, task: Task) {
        while self.by_id.len() >= MAX_TASKS {
            let Some(pos) = self
                .order
                .iter()
                .position(|i| !matches!(self.by_id.get(i).map(|t| &t.state), Some(TaskState::Working)))
            else {
                break;
            };
            if let Some(old) = self.order.remove(pos) {
                self.by_id.remove(&old);
            }
        }
        self.order.push_back(id.clone());
        self.by_id.insert(id, task);
    }
}

#[derive(Clone)]
pub struct AppState {
    cfg: Arc<ServeConfig>,
    tasks: Arc<Mutex<Tasks>>,
    slots: Arc<Semaphore>,
    http: reqwest::Client,
}

impl AppState {
    pub fn new(cfg: ServeConfig) -> Self {
        let slots = Arc::new(Semaphore::new(cfg.max_concurrent.max(1)));
        AppState {
            cfg: Arc::new(cfg),
            tasks: Arc::new(Mutex::new(Tasks::default())),
            slots,
            http: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .timeout(Duration::from_secs(10))
                .build()
                .unwrap_or_default(),
        }
    }

    fn catalog(&self) -> Catalog {
        scan(&self.cfg.playbooks)
    }
}

fn new_id() -> String {
    use rand::RngCore;
    let mut b = [0u8; 8];
    rand::thread_rng().fill_bytes(&mut b);
    // A decimal id, like a server execution id.
    (u64::from_be_bytes(b) >> 1).to_string()
}

fn new_token() -> String {
    use rand::RngCore;
    let mut b = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut b);
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// Run one exposed playbook with `args` as its workload, off the async
/// runtime (the runner is synchronous).
async fn run_playbook(file: PathBuf, args: Map<String, Value>, verbose: bool) -> Result<RunOutcome, String> {
    let vars: HashMap<String, String> = args
        .into_iter()
        .map(|(k, v)| {
            let s = match v {
                Value::String(s) => s,
                other => other.to_string(),
            };
            (format!("workload.{k}"), defang_template_markers(&s))
        })
        .collect();
    tokio::task::spawn_blocking(move || {
        PlaybookRunner::new(file)
            .with_variables(vars)
            .with_verbose(verbose)
            .with_quiet(!verbose)
            .run()
    })
    .await
    .map_err(|e| format!("run panicked: {e}"))?
    .map_err(|e| format!("{e:#}"))
}

fn outcome_to_state(outcome: Result<RunOutcome, String>, result_step: Option<&str>) -> TaskState {
    match outcome {
        Ok(o) if o.status == "ok" => match select_result(&o, result_step) {
            Ok(v) => TaskState::Completed(redact(&v)),
            Err(e) => TaskState::Failed(e),
        },
        Ok(o) => TaskState::Failed(o.error.unwrap_or_else(|| "the playbook failed".into())),
        Err(e) => TaskState::Failed(e),
    }
}

fn task_view(id: &str, t: &Task) -> Value {
    let mut v = json!({"task_id": id, "agent": t.agent});
    match &t.state {
        TaskState::Working => v["status"] = json!("working"),
        TaskState::Completed(r) => {
            v["status"] = json!("completed");
            v["result"] = r.clone();
        }
        TaskState::Failed(e) => {
            v["status"] = json!("failed");
            v["error"] = json!(e);
        }
    }
    v
}

/// Start a task: reserve a slot, record it, run it in the background.
/// `on_done` runs once with the final view (the callback).
fn start_task(
    state: &AppState,
    agent: String,
    file: PathBuf,
    args: Map<String, Value>,
    result_step: Option<String>,
    on_done: Option<(String, String)>,
) -> Result<String, Response> {
    let permit = state
        .slots
        .clone()
        .try_acquire_owned()
        .map_err(|_| error(StatusCode::SERVICE_UNAVAILABLE, "all run slots are busy; retry later"))?;
    let id = new_id();
    state.tasks.lock().unwrap().insert(
        id.clone(),
        Task {
            agent: agent.clone(),
            state: TaskState::Working,
        },
    );
    let st = state.clone();
    let task_id = id.clone();
    let span = tracing::info_span!("serve.run", agent = %agent, task_id = %task_id);
    tokio::spawn(
        async move {
            let outcome = run_playbook(file, args, st.cfg.verbose).await;
            drop(permit);
            let new_state = outcome_to_state(outcome, result_step.as_deref());
            let view = {
                let mut tasks = st.tasks.lock().unwrap();
                let entry = tasks.by_id.entry(task_id.clone()).or_insert(Task {
                    agent: agent.clone(),
                    state: TaskState::Working,
                });
                entry.state = new_state;
                task_view(&task_id, entry)
            };
            tracing::info!(task_id = %task_id, status = %view["status"], "task finished");
            if let Some((url, token)) = on_done {
                deliver_callback(&st.http, &url, &token, &view).await;
            }
        }
        .instrument(span),
    );
    Ok(id)
}

async fn deliver_callback(http: &reqwest::Client, url: &str, token: &str, view: &Value) {
    for attempt in 0..4u32 {
        match http.post(url).bearer_auth(token).json(view).send().await {
            Ok(r) if r.status().is_success() => return,
            Ok(r) => tracing::warn!(status = %r.status(), attempt, "callback refused"),
            Err(e) => tracing::warn!(error = %e, attempt, "callback failed"),
        }
        tokio::time::sleep(Duration::from_secs(1 << attempt)).await;
    }
}

pub fn origin_of(url: &str) -> Option<String> {
    let u = reqwest::Url::parse(url).ok()?;
    if !matches!(u.scheme(), "http" | "https") {
        return None;
    }
    let host = u.host_str()?;
    Some(match u.port() {
        Some(p) => format!("{}://{}:{}", u.scheme(), host, p),
        None => format!("{}://{}", u.scheme(), host),
    })
}

// ---------------------------------------------------------------------------
// HTTP
// ---------------------------------------------------------------------------

fn error(status: StatusCode, message: impl Into<String>) -> Response {
    (status, Json(json!({"error": message.into()}))).into_response()
}

fn authorized(state: &AppState, headers: &HeaderMap) -> bool {
    let Some(want) = &state.cfg.token else { return true };
    let got = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or("");
    // Constant-time compare.
    got.len() == want.len() && got.bytes().zip(want.bytes()).fold(0u8, |a, (x, y)| a | (x ^ y)) == 0
}

pub fn router(state: AppState) -> Router {
    let mut r = Router::new().route("/healthz", get(|| async { Json(json!({"status": "ok"})) }));
    if state.cfg.mcp {
        r = r.route("/mcp", post(mcp_post).get(|| async { StatusCode::METHOD_NOT_ALLOWED }));
    }
    if state.cfg.agents {
        r = r
            .route("/api/agents", get(list_cards))
            .route("/api/agents/cards/*path", get(get_card))
            .route("/api/agents/send", post(send))
            .route("/api/agents/tasks/:id", get(get_task));
    }
    r.layer(axum::extract::DefaultBodyLimit::max(MAX_BODY_BYTES))
        .with_state(state)
}

fn rpc_result(id: &Value, result: Value) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "result": result})
}

fn rpc_error(id: &Value, code: i64, message: impl Into<String>) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": message.into()}})
}

fn tool_result(value: &Value, is_error: bool, meta: Value) -> Value {
    let text = match value {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    };
    let mut out = json!({"content": [{"type": "text", "text": text}], "isError": is_error, "_meta": meta});
    if value.is_object() {
        out["structuredContent"] = value.clone();
    }
    out
}

async fn mcp_post(State(state): State<AppState>, headers: HeaderMap, body: Bytes) -> Response {
    if !authorized(&state, &headers) {
        return error(StatusCode::UNAUTHORIZED, "missing or wrong bearer token");
    }
    let msg: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => return Json(rpc_error(&Value::Null, PARSE_ERROR, format!("parse error: {e}"))).into_response(),
    };
    match handle_rpc(&state, &msg).await {
        Some(reply) => Json(reply).into_response(),
        None => StatusCode::ACCEPTED.into_response(),
    }
}

/// One JSON-RPC message. `None` for a notification.
pub async fn handle_rpc(state: &AppState, msg: &Value) -> Option<Value> {
    let method = msg.get("method").and_then(Value::as_str).unwrap_or("").to_string();
    handle_rpc_inner(state, msg)
        .instrument(tracing::info_span!("serve.mcp", method = %method))
        .await
}

async fn handle_rpc_inner(state: &AppState, msg: &Value) -> Option<Value> {
    let Some(method) = msg.get("method").and_then(Value::as_str) else {
        return Some(rpc_error(
            msg.get("id").unwrap_or(&Value::Null),
            INVALID_REQUEST,
            "`method` is required",
        ));
    };
    let Some(id) = msg.get("id").cloned() else {
        return None; // notification
    };
    let params = msg.get("params").cloned().unwrap_or(json!({}));
    Some(match method {
        "initialize" => {
            let asked = params.get("protocolVersion").and_then(Value::as_str).unwrap_or("");
            let version = SUPPORTED_PROTOCOL_VERSIONS
                .iter()
                .find(|v| **v == asked)
                .unwrap_or(&SUPPORTED_PROTOCOL_VERSIONS[0]);
            rpc_result(
                &id,
                json!({
                    "protocolVersion": version,
                    "capabilities": {"tools": {"listChanged": false}},
                    "serverInfo": {"name": "noetl-serve", "version": env!("CARGO_PKG_VERSION")},
                }),
            )
        }
        "ping" => rpc_result(&id, json!({})),
        "tools/list" => {
            let cat = state.catalog();
            let mut tools: Vec<Value> = cat
                .playbooks
                .iter()
                .filter_map(|p| p.mcp.as_ref().map(|m| (p, m)))
                .map(|(p, m)| {
                    json!({
                        "name": m.tool,
                        "description": m.description,
                        "inputSchema": m.input_schema,
                        "_meta": {"noetl/playbook": p.path, "noetl/mode": if m.mode == Mode::Async { "async" } else { "sync" }},
                    })
                })
                .collect();
            let id_schema = json!({"type": "object", "properties": {"execution_id": {"type": "string"}}, "required": ["execution_id"]});
            tools.push(json!({"name": HELPER_STATUS, "description": "Status of an execution started by a mode: async tool.", "inputSchema": id_schema}));
            tools.push(json!({"name": HELPER_RESULT, "description": "Result of a finished execution started by a mode: async tool.", "inputSchema": id_schema}));
            let not_served: Vec<Value> = cat
                .problems
                .iter()
                .map(|p| json!({"source": p.source, "reason": p.reason}))
                .collect();
            rpc_result(&id, json!({"tools": tools, "_meta": {"noetl/not_served": not_served}}))
        }
        "tools/call" => {
            let Some(name) = params.get("name").and_then(Value::as_str) else {
                return Some(rpc_error(&id, INVALID_PARAMS, "`params.name` is required"));
            };
            let args = params.get("arguments");
            if name.starts_with(HELPER_PREFIX) {
                return Some(match call_helper(state, name, args) {
                    Ok(v) => rpc_result(&id, v),
                    Err(e) => rpc_error(&id, INVALID_PARAMS, e),
                });
            }
            let cat = state.catalog();
            let Some(p) = cat.mcp_tool(name) else {
                return Some(rpc_error(&id, INVALID_PARAMS, format!("unknown tool `{name}`")));
            };
            let m = p.mcp.clone().unwrap_or_else(|| unreachable!());
            let args = match check_arguments(&m.input_schema, args) {
                Ok(a) => a,
                Err(e) => return Some(rpc_result(&id, tool_result(&json!(e), true, json!({})))),
            };
            let meta = |eid: &str| json!({"noetl/execution_id": eid, "noetl/playbook": p.path});
            match m.mode {
                Mode::Async => {
                    match start_task(state, p.path.clone(), p.file.clone(), args, m.result_step.clone(), None) {
                        Ok(tid) => rpc_result(
                            &id,
                            tool_result(&json!({"status": "started", "execution_id": tid}), false, meta(&tid)),
                        ),
                        Err(_) => rpc_result(
                            &id,
                            tool_result(&json!("all run slots are busy; retry later"), true, json!({})),
                        ),
                    }
                }
                Mode::Sync => {
                    let Ok(permit) = state.slots.clone().try_acquire_owned() else {
                        return Some(rpc_result(
                            &id,
                            tool_result(&json!("all run slots are busy; retry later"), true, json!({})),
                        ));
                    };
                    let run = run_playbook(p.file.clone(), args, state.cfg.verbose);
                    let outcome = tokio::time::timeout(Duration::from_secs(m.timeout), run).await;
                    drop(permit);
                    match outcome {
                        Err(_) => rpc_result(
                            &id,
                            tool_result(
                                &json!(format!(
                                    "the playbook did not finish within {}s; use mode: async for long tools",
                                    m.timeout
                                )),
                                true,
                                json!({}),
                            ),
                        ),
                        Ok(o) => match outcome_to_state(o, m.result_step.as_deref()) {
                            TaskState::Completed(v) => {
                                rpc_result(&id, tool_result(&v, false, json!({"noetl/playbook": p.path})))
                            }
                            TaskState::Failed(e) => {
                                rpc_result(&id, tool_result(&json!(e), true, json!({"noetl/playbook": p.path})))
                            }
                            TaskState::Working => unreachable!(),
                        },
                    }
                }
            }
        }
        other => rpc_error(&id, METHOD_NOT_FOUND, format!("method `{other}` is not supported")),
    })
}

fn call_helper(state: &AppState, name: &str, args: Option<&Value>) -> Result<Value, String> {
    let eid = args
        .and_then(|a| a.get("execution_id"))
        .and_then(|v| {
            v.as_str()
                .map(str::to_string)
                .or_else(|| v.as_u64().map(|n| n.to_string()))
        })
        .ok_or("`execution_id` is required")?;
    let tasks = state.tasks.lock().unwrap();
    let t = tasks
        .by_id
        .get(&eid)
        .ok_or_else(|| format!("no execution `{eid}` on this server"))?;
    let view = task_view(&eid, t);
    match name {
        HELPER_STATUS => Ok(tool_result(
            &json!({"execution_id": eid, "status": view["status"]}),
            false,
            json!({}),
        )),
        HELPER_RESULT => match &t.state {
            TaskState::Completed(v) => Ok(tool_result(v, false, json!({"noetl/execution_id": eid}))),
            TaskState::Failed(e) => Ok(tool_result(&json!(e), true, json!({"noetl/execution_id": eid}))),
            TaskState::Working => Ok(tool_result(
                &json!({"execution_id": eid, "status": "working"}),
                false,
                json!({}),
            )),
        },
        other => Err(format!("unknown helper `{other}`")),
    }
}

fn card(p: &Exposed) -> Value {
    let a = p.agent.as_ref().unwrap_or_else(|| unreachable!());
    json!({
        "name": a.name,
        "description": a.description,
        "version": a.version,
        "agent": p.path,
        "skills": a.skills,
        "inputSchema": a.input_schema,
        "defaultInputModes": ["application/json"],
        "defaultOutputModes": ["application/json"],
        "capabilities": {"callbacks": true, "streaming": false},
        "endpoints": {"send": "/api/agents/send", "task": "/api/agents/tasks/{task_id}"},
    })
}

async fn list_cards(State(state): State<AppState>, headers: HeaderMap) -> Response {
    if !authorized(&state, &headers) {
        return error(StatusCode::UNAUTHORIZED, "missing or wrong bearer token");
    }
    let cat = state.catalog();
    let cards: Vec<Value> = cat.playbooks.iter().filter(|p| p.agent.is_some()).map(card).collect();
    let not_served: Vec<Value> = cat
        .problems
        .iter()
        .map(|p| json!({"source": p.source, "reason": p.reason}))
        .collect();
    Json(json!({"agents": cards, "not_served": not_served})).into_response()
}

async fn get_card(State(state): State<AppState>, headers: HeaderMap, AxPath(path): AxPath<String>) -> Response {
    if !authorized(&state, &headers) {
        return error(StatusCode::UNAUTHORIZED, "missing or wrong bearer token");
    }
    match state.catalog().agent(&path) {
        Some(p) => Json(card(p)).into_response(),
        None => error(StatusCode::NOT_FOUND, "no such agent"),
    }
}

async fn send(State(state): State<AppState>, headers: HeaderMap, Json(body): Json<Value>) -> Response {
    if !authorized(&state, &headers) {
        return error(StatusCode::UNAUTHORIZED, "missing or wrong bearer token");
    }
    let Some(agent_path) = body.get("agent").and_then(Value::as_str) else {
        return error(StatusCode::BAD_REQUEST, "`agent` (the playbook path) is required");
    };
    let cat = state.catalog();
    let Some(p) = cat.agent(agent_path) else {
        return error(StatusCode::NOT_FOUND, "no such agent");
    };
    let a = p.agent.as_ref().unwrap_or_else(|| unreachable!());
    let input = match check_arguments(&a.input_schema, body.get("input")) {
        Ok(m) => m,
        Err(e) => return error(StatusCode::UNPROCESSABLE_ENTITY, e),
    };
    let callback = match body.get("callback_url").and_then(Value::as_str) {
        None => None,
        Some(url) => {
            let Some(origin) = origin_of(url) else {
                return error(StatusCode::BAD_REQUEST, "`callback_url` must be an http(s) URL");
            };
            if !state
                .cfg
                .callback_origins
                .iter()
                .any(|o| o.trim_end_matches('/') == origin)
            {
                return error(
                    StatusCode::FORBIDDEN,
                    format!("callback origin {origin} is not allowed (serve --callback-origin)"),
                );
            }
            Some((url.to_string(), new_token()))
        }
    };
    let token = callback.as_ref().map(|(_, t)| t.clone());
    match start_task(
        &state,
        p.path.clone(),
        p.file.clone(),
        input,
        a.result_step.clone(),
        callback,
    ) {
        Ok(id) => {
            let mut reply = json!({"task_id": id, "status": "submitted"});
            if let Some(t) = token {
                reply["callback_token"] = json!(t);
            }
            (StatusCode::ACCEPTED, Json(reply)).into_response()
        }
        Err(r) => r,
    }
}

async fn get_task(State(state): State<AppState>, headers: HeaderMap, AxPath(id): AxPath<String>) -> Response {
    if !authorized(&state, &headers) {
        return error(StatusCode::UNAUTHORIZED, "missing or wrong bearer token");
    }
    let tasks = state.tasks.lock().unwrap();
    match tasks.by_id.get(&id) {
        Some(t) => Json(task_view(&id, t)).into_response(),
        None => error(StatusCode::NOT_FOUND, "no such task"),
    }
}

/// Bind and serve until `shutdown` resolves.
pub async fn serve(cfg: ServeConfig, shutdown: impl std::future::Future<Output = ()> + Send + 'static) -> Result<()> {
    if !cfg.mcp && !cfg.agents {
        anyhow::bail!("nothing to serve: both --no-mcp and --no-agents were given");
    }
    if !cfg.playbooks.is_dir() {
        anyhow::bail!("playbook directory {} does not exist", cfg.playbooks.display());
    }
    if !cfg.listen.ip().is_loopback() && cfg.token.is_none() {
        anyhow::bail!(
            "refusing to listen on {} without --token-file: a non-loopback listener must require a bearer token",
            cfg.listen
        );
    }
    let cat = scan(&cfg.playbooks);
    let listener = tokio::net::TcpListener::bind(cfg.listen)
        .await
        .with_context(|| format!("bind {}", cfg.listen))?;
    let addr = listener.local_addr()?;
    eprintln!("🛰  noetl serve — {} on http://{}", cfg.playbooks.display(), addr);
    if cfg.mcp {
        let n = cat.playbooks.iter().filter(|p| p.mcp.is_some()).count();
        eprintln!("   mcp    : POST /mcp ({n} tool(s))");
    }
    if cfg.agents {
        let n = cat.playbooks.iter().filter(|p| p.agent.is_some()).count();
        eprintln!(
            "   agents : /api/agents ({n} agent(s)), callbacks to {} origin(s)",
            cfg.callback_origins.len()
        );
    }
    for p in &cat.problems {
        eprintln!("   not served: {} — {}", p.source, p.reason);
    }
    axum::serve(listener, router(AppState::new(cfg)))
        .with_graceful_shutdown(shutdown)
        .await
        .context("serve")
}

/// Read a bearer token file (trimmed; must not be empty).
pub fn read_token_file(path: &Path) -> Result<String> {
    let t = std::fs::read_to_string(path).with_context(|| format!("read token file {}", path.display()))?;
    let t = t.trim().to_string();
    if t.len() < 16 {
        anyhow::bail!("token file {} must hold at least 16 characters", path.display());
    }
    Ok(t)
}

#[cfg(test)]
mod tests;

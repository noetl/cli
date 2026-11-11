use super::*;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const ECHO: &str = r#"
apiVersion: noetl.io/v2
kind: Playbook
metadata:
  name: echo
  path: demo/echo
  expose:
    mcp:
      tool: demo_echo
      description: Echo a question back.
      input_schema:
        type: object
        properties: { question: { type: string } }
        required: [question]
    agent:
      card: { name: echoer, description: Echoes questions., version: "1" }
workflow:
  - step: start
    next: [ { step: say } ]
  - step: say
    tool: { kind: shell, cmds: [ "echo 'answer: {{ workload.question }}'" ] }
"#;

const SLOW: &str = r#"
apiVersion: noetl.io/v2
kind: Playbook
metadata:
  name: slow
  expose:
    mcp:
      tool: slow_report
      description: A long report.
      mode: async
      input_schema: { type: object, properties: {} }
workflow:
  - step: start
    next: [ { step: work } ]
  - step: work
    tool: { kind: shell, cmds: [ "sleep 0.3", "echo '{\"rows\": 3, \"api_token\": \"tok-123\"}'" ] }
"#;

const BROKEN: &str = r#"
apiVersion: noetl.io/v2
kind: Playbook
metadata:
  name: broken
  expose:
    mcp: { tool: broken, description: no schema }
workflow:
  - step: start
"#;

const HIDDEN: &str = r#"
apiVersion: noetl.io/v2
kind: Playbook
metadata: { name: hidden }
workflow:
  - step: start
    tool: { kind: shell, cmds: [ "echo hidden" ] }
"#;

fn dir() -> tempfile::TempDir {
    let d = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(d.path().join("demo")).unwrap();
    std::fs::write(d.path().join("demo/echo.yaml"), ECHO).unwrap();
    std::fs::write(d.path().join("slow.yaml"), SLOW).unwrap();
    std::fs::write(d.path().join("broken.yaml"), BROKEN).unwrap();
    std::fs::write(d.path().join("hidden.yaml"), HIDDEN).unwrap();
    d
}

fn cfg(d: &Path) -> ServeConfig {
    ServeConfig {
        playbooks: d.to_path_buf(),
        listen: "127.0.0.1:0".parse().unwrap(),
        mcp: true,
        agents: true,
        token: None,
        callback_origins: vec![],
        max_concurrent: 4,
        verbose: false,
        task_db: None,
    }
}

async fn start(cfg: ServeConfig) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router(AppState::new(cfg))).await.unwrap();
    });
    format!("http://{addr}")
}

async fn rpc(base: &str, method: &str, params: Value) -> Value {
    reqwest::Client::new()
        .post(format!("{base}/mcp"))
        .json(&json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

#[test]
fn scan_serves_opted_in_playbooks_and_reports_the_rest() {
    let d = dir();
    let cat = scan(d.path());
    let paths: Vec<&str> = cat.playbooks.iter().map(|p| p.path.as_str()).collect();
    assert_eq!(paths, vec!["demo/echo", "slow"]);
    assert_eq!(cat.problems.len(), 1);
    assert!(cat.problems[0].reason.contains("input_schema"), "{:?}", cat.problems);
}

#[test]
fn check_arguments_is_the_allowlist() {
    let schema = json!({"type": "object", "properties": {"q": {"type": "string"}}, "required": ["q"]});
    assert!(check_arguments(&schema, Some(&json!({"q": "x"}))).is_ok());
    assert!(check_arguments(&schema, Some(&json!({})))
        .unwrap_err()
        .contains("missing"));
    assert!(check_arguments(&schema, Some(&json!({"q": "x", "secret": 1})))
        .unwrap_err()
        .contains("not declared"));
    assert!(check_arguments(&schema, Some(&json!({"q": 1})))
        .unwrap_err()
        .contains("type"));
    let open = json!({"type": "object", "additionalProperties": true});
    assert!(check_arguments(&open, Some(&json!({"any": 1}))).is_ok());
}

#[test]
fn redact_masks_credential_fields() {
    let v = redact(&json!({"rows": 3, "api_token": "t", "nested": [{"password": "p", "ok": 1}]}));
    assert_eq!(
        v,
        json!({"rows": 3, "api_token": "***", "nested": [{"password": "***", "ok": 1}]})
    );
}

#[test]
fn origin_of_parses_scheme_host_port() {
    assert_eq!(
        origin_of("http://127.0.0.1:9000/cb?x=1").as_deref(),
        Some("http://127.0.0.1:9000")
    );
    assert_eq!(origin_of("https://a.example/cb").as_deref(), Some("https://a.example"));
    assert_eq!(origin_of("file:///etc/passwd"), None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mcp_initialize_list_and_sync_call() {
    let d = dir();
    let base = start(cfg(d.path())).await;
    let init = rpc(&base, "initialize", json!({"protocolVersion": "2025-03-26"})).await;
    assert_eq!(init["result"]["protocolVersion"], "2025-03-26");

    let list = rpc(&base, "tools/list", json!({})).await;
    let names: Vec<&str> = list["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, vec!["demo_echo", "slow_report", HELPER_STATUS, HELPER_RESULT]);
    assert_eq!(list["result"]["_meta"]["noetl/not_served"][0]["source"], "broken");

    let call = rpc(
        &base,
        "tools/call",
        json!({"name": "demo_echo", "arguments": {"question": "why"}}),
    )
    .await;
    assert_eq!(call["result"]["isError"], false, "{call}");
    assert_eq!(call["result"]["content"][0]["text"], "answer: why");

    // Arguments are data: a template in them is not expanded by the playbook.
    let call = rpc(
        &base,
        "tools/call",
        json!({"name": "demo_echo", "arguments": {"question": "{{ workload.question }}"}}),
    )
    .await;
    assert_eq!(call["result"]["content"][0]["text"], "answer: { { workload.question }}");

    let refused = rpc(
        &base,
        "tools/call",
        json!({"name": "demo_echo", "arguments": {"question": "q", "x": 1}}),
    )
    .await;
    assert_eq!(refused["result"]["isError"], true);
    let unknown = rpc(&base, "tools/call", json!({"name": "hidden", "arguments": {}})).await;
    assert_eq!(
        unknown["error"]["code"], INVALID_PARAMS,
        "an unexposed playbook is not a tool"
    );

    // Notifications get 202 and no body.
    let r = reqwest::Client::new()
        .post(format!("{base}/mcp"))
        .json(&json!({"jsonrpc": "2.0", "method": "notifications/initialized"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 202);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mcp_async_tool_and_helpers_and_redaction() {
    let d = dir();
    let base = start(cfg(d.path())).await;
    let started = rpc(&base, "tools/call", json!({"name": "slow_report", "arguments": {}})).await;
    let eid = started["result"]["structuredContent"]["execution_id"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(started["result"]["structuredContent"]["status"], "started");
    let mut result = Value::Null;
    for _ in 0..50 {
        let s = rpc(
            &base,
            "tools/call",
            json!({"name": HELPER_STATUS, "arguments": {"execution_id": eid}}),
        )
        .await;
        if s["result"]["structuredContent"]["status"] == "completed" {
            result = rpc(
                &base,
                "tools/call",
                json!({"name": HELPER_RESULT, "arguments": {"execution_id": eid}}),
            )
            .await;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(result["result"]["structuredContent"]["rows"], 3, "{result}");
    assert_eq!(result["result"]["structuredContent"]["api_token"], "***");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn agent_card_send_poll_and_callback() {
    let d = dir();
    let cb = MockServer::start().await;
    let mut c = cfg(d.path());
    c.callback_origins = vec![cb.uri()];
    let base = start(c).await;
    let http = reqwest::Client::new();

    let cards: Value = http
        .get(format!("{base}/api/agents"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(cards["agents"][0]["agent"], "demo/echo");
    assert_eq!(cards["agents"][0]["inputSchema"]["required"], json!(["question"]));
    let card: Value = http
        .get(format!("{base}/api/agents/cards/demo/echo"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(card["name"], "echoer");

    let bad = http
        .post(format!("{base}/api/agents/send"))
        .json(&json!({"agent": "demo/echo", "input": {}}))
        .send()
        .await
        .unwrap();
    assert_eq!(bad.status(), 422);
    let forbidden = http
        .post(format!("{base}/api/agents/send"))
        .json(&json!({"agent": "demo/echo", "input": {"question": "q"}, "callback_url": "http://10.0.0.1/cb"}))
        .send()
        .await
        .unwrap();
    assert_eq!(forbidden.status(), 403);

    let sent: Value = http
        .post(format!("{base}/api/agents/send"))
        .json(
            &json!({"agent": "demo/echo", "input": {"question": "status"}, "callback_url": format!("{}/cb", cb.uri())}),
        )
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let id = sent["task_id"].as_str().unwrap().to_string();
    let token = sent["callback_token"].as_str().unwrap().to_string();
    Mock::given(method("POST"))
        .and(path("/cb"))
        .and(header("authorization", format!("Bearer {token}").as_str()))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&cb)
        .await;

    let mut view = Value::Null;
    for _ in 0..50 {
        view = http
            .get(format!("{base}/api/agents/tasks/{id}"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if view["status"] != "working" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(view["status"], "completed", "{view}");
    assert_eq!(view["result"], "answer: status");
    // The callback carried the final view with the task's own token.
    for _ in 0..30 {
        if !cb.received_requests().await.unwrap().is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let got = cb.received_requests().await.unwrap();
    assert_eq!(got.len(), 1);
    let body: Value = serde_json::from_slice(&got[0].body).unwrap();
    assert_eq!(body["task_id"], id);
    assert_eq!(body["result"], "answer: status");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_token_guards_every_route_but_healthz() {
    let d = dir();
    let mut c = cfg(d.path());
    c.token = Some("0123456789abcdef-token".into());
    let base = start(c).await;
    let http = reqwest::Client::new();
    assert_eq!(http.get(format!("{base}/healthz")).send().await.unwrap().status(), 200);
    assert_eq!(
        http.get(format!("{base}/api/agents")).send().await.unwrap().status(),
        401
    );
    let no = http
        .post(format!("{base}/mcp"))
        .json(&json!({"jsonrpc": "2.0", "id": 1, "method": "ping"}))
        .send()
        .await
        .unwrap();
    assert_eq!(no.status(), 401);
    let yes = http
        .post(format!("{base}/mcp"))
        .bearer_auth("0123456789abcdef-token")
        .json(&json!({"jsonrpc": "2.0", "id": 1, "method": "ping"}))
        .send()
        .await
        .unwrap();
    assert_eq!(yes.status(), 200);
}

#[tokio::test]
async fn a_non_loopback_listener_needs_a_token() {
    let d = dir();
    let mut c = cfg(d.path());
    c.listen = "0.0.0.0:0".parse().unwrap();
    let err = serve(c, async {}).await.unwrap_err().to_string();
    assert!(err.contains("--token-file"), "{err}");
}

// ---------------------------------------------------------------------------
// --task-db: durable tasks
// ---------------------------------------------------------------------------

fn durable(d: &Path, db: &Path) -> ServeConfig {
    ServeConfig {
        task_db: Some(db.to_path_buf()),
        ..cfg(d)
    }
}

async fn poll_store(state: &AppState, id: &str) -> Value {
    let mut view = Value::Null;
    for _ in 0..80 {
        let t = state.tasks.get(id).unwrap().expect("task row");
        view = task_view(id, &t);
        if view["status"] != "working" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    view
}

fn echo_input(question: &str, callback: Option<(String, String)>) -> store::TaskInput {
    let mut args = Map::new();
    args.insert("question".into(), json!(question));
    store::TaskInput {
        agent: "demo/echo".into(),
        args,
        result_step: None,
        callback,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_finished_task_answers_after_a_restart() {
    let d = dir();
    let db = d.path().join("state/tasks.db");
    let base = start(durable(d.path(), &db)).await;
    let http = reqwest::Client::new();
    let sent: Value = http
        .post(format!("{base}/api/agents/send"))
        .json(&json!({"agent": "demo/echo", "input": {"question": "durable"}}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let id = sent["task_id"].as_str().unwrap().to_string();
    let mut view = Value::Null;
    for _ in 0..50 {
        view = http
            .get(format!("{base}/api/agents/tasks/{id}"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if view["status"] != "working" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(view["result"], "answer: durable", "{view}");

    // A new process on the same file: the task is still there.
    let base2 = start(durable(d.path(), &db)).await;
    let again: Value = http
        .get(format!("{base2}/api/agents/tasks/{id}"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(again, view);

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&db).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "the task db holds inputs and callback tokens");
    }
    // The default store still forgets: a memory server does not know the id.
    let mem = start(cfg(d.path())).await;
    let r = http.get(format!("{mem}/api/agents/tasks/{id}")).send().await.unwrap();
    assert_eq!(r.status(), 404);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_task_working_at_the_crash_is_rerun_and_its_callback_delivered() {
    let d = dir();
    let db = d.path().join("tasks.db");
    let cb = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/cb"))
        .and(header("authorization", "Bearer tok-1"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&cb)
        .await;
    // The previous process recorded the task, then died before finishing it.
    {
        let s = store::TaskStore::open_sqlite(&db).unwrap();
        s.insert(
            "1001",
            &echo_input("after crash", Some((format!("{}/cb", cb.uri()), "tok-1".into()))),
        )
        .unwrap();
    }
    let state = AppState::open(durable(d.path(), &db)).unwrap();
    assert_eq!(state.resume().unwrap(), (1, 0));
    let view = poll_store(&state, "1001").await;
    assert_eq!(view["status"], "completed", "{view}");
    assert_eq!(view["result"], "answer: after crash");
    for _ in 0..30 {
        if !cb.received_requests().await.unwrap().is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let got = cb.received_requests().await.unwrap();
    assert_eq!(got.len(), 1, "the owed callback is delivered once");
    // Delivered is recorded: another restart owes nothing.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let again = AppState::open(durable(d.path(), &db)).unwrap();
    assert_eq!(again.resume().unwrap(), (0, 0));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_undelivered_callback_of_a_finished_task_is_delivered_after_a_restart() {
    let d = dir();
    let db = d.path().join("tasks.db");
    let cb = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/cb"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&cb)
        .await;
    {
        let s = store::TaskStore::open_sqlite(&db).unwrap();
        s.insert(
            "2002",
            &echo_input("q", Some((format!("{}/cb", cb.uri()), "tok-2".into()))),
        )
        .unwrap();
        s.finish(
            "2002",
            "demo/echo",
            TaskState::Completed(json!("done before the crash")),
        )
        .unwrap();
    }
    let state = AppState::open(durable(d.path(), &db)).unwrap();
    assert_eq!(state.resume().unwrap(), (0, 1));
    for _ in 0..30 {
        if !cb.received_requests().await.unwrap().is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let got = cb.received_requests().await.unwrap();
    assert_eq!(got.len(), 1);
    let body: Value = serde_json::from_slice(&got[0].body).unwrap();
    assert_eq!(body["task_id"], "2002");
    assert_eq!(body["result"], "done before the crash");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_task_that_keeps_dying_is_failed_by_name_after_the_attempt_limit() {
    let d = dir();
    let db = d.path().join("tasks.db");
    {
        let s = store::TaskStore::open_sqlite(&db).unwrap();
        s.insert("3003", &echo_input("q", None)).unwrap();
        // Two restarts that each died again before the task finished.
        for _ in 0..(store::MAX_ATTEMPTS - 1) {
            let n = s.recover().unwrap().len();
            assert_eq!(n, 1);
        }
    }
    let state = AppState::open(durable(d.path(), &db)).unwrap();
    assert_eq!(state.resume().unwrap(), (0, 0));
    let view = task_view("3003", &state.tasks.get("3003").unwrap().unwrap());
    assert_eq!(view["status"], "failed");
    assert!(
        view["error"]
            .as_str()
            .unwrap()
            .contains("interrupted by a restart 3 time(s)"),
        "{view}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_resumed_task_whose_agent_is_gone_fails_by_name() {
    let d = dir();
    let db = d.path().join("tasks.db");
    {
        let s = store::TaskStore::open_sqlite(&db).unwrap();
        let mut input = echo_input("q", None);
        input.agent = "demo/removed".into();
        s.insert("4004", &input).unwrap();
    }
    let state = AppState::open(durable(d.path(), &db)).unwrap();
    assert_eq!(state.resume().unwrap(), (1, 0));
    let view = task_view("4004", &state.tasks.get("4004").unwrap().unwrap());
    assert_eq!(view["status"], "failed");
    assert!(view["error"].as_str().unwrap().contains("no longer served"), "{view}");
}

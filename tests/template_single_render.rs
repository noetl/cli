//! Local runs render each step's templates exactly once (noetl/ai-meta#477).
//!
//! The runner renders shell / http / rhai config with `render_template`, then
//! the noetl-tools tool used to render it again with minijinja, so a
//! `{{ … }}` that arrived as *data* (here `workload.a`, whose value is the
//! literal text `{{ workload.secret }}`) was evaluated a second time: blank
//! for shell and http, the secret itself for rhai.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};

use noetl::PlaybookRunner;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const LITERAL: &str = "{{ workload.secret }}";

fn write_playbook(name: &str, steps: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("noetl-single-render-{}-{}", name, std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let file = dir.join(format!("{name}.yaml"));
    std::fs::write(
        &file,
        format!(
            r#"
apiVersion: noetl.io/v2
kind: Playbook
metadata: {{ name: {name} }}
workload: {{ secret: S3CRET, a: "{{{{ workload.secret }}}}" }}
workflow:
  - step: start
    next: [ {{ step: show }} ]
{steps}"#
        ),
    )
    .unwrap();
    file
}

fn run(file: &Path, vars: HashMap<String, String>) -> BTreeMap<String, String> {
    let outcome = PlaybookRunner::new(file.to_path_buf())
        .with_variables(vars)
        .with_quiet(true)
        .run()
        .expect("playbook runs");
    assert_eq!(outcome.status, "ok", "{:?}", outcome.error);
    outcome.step_results
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shell_probe_prints_data_markers_literally_every_time() {
    let file = write_playbook(
        "reexpand_probe",
        r#"  - step: show
    tool: { kind: shell, cmds: [ 'echo "x {{ workload.a }}"' ] }
"#,
    );
    for i in 0..30 {
        let results = run(&file, HashMap::new());
        assert_eq!(results["show"], format!("x {LITERAL}"), "run {i}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shell_still_renders_templates() {
    let file = write_playbook(
        "shell_renders",
        r#"  - step: show
    tool: { kind: shell, cmds: [ 'echo "x {{ workload.secret }}"' ] }
    next: [ { step: again } ]
  - step: again
    tool: { kind: shell, cmds: [ 'echo "got {{ show.result }}"' ] }
"#,
    );
    let results = run(&file, HashMap::new());
    assert_eq!(results["show"], "x S3CRET");
    assert_eq!(results["again"], "got x S3CRET");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rhai_does_not_expand_data_markers() {
    let file = write_playbook(
        "rhai_probe",
        r#"  - step: show
    tool: { kind: rhai, code: 'let v = "x {{ workload.a }}"; v' }
    next: [ { step: plain } ]
  - step: plain
    tool: { kind: rhai, code: 'let v = "y {{ workload.secret }}"; v' }
"#,
    );
    let results = run(&file, HashMap::new());
    assert_eq!(results["show"], format!("x {LITERAL}"));
    assert_eq!(results["plain"], "y S3CRET");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_sends_data_markers_literally() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/echo"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({ "ok": true })))
        .mount(&server)
        .await;

    let file = write_playbook(
        "http_probe",
        r#"  - step: show
    tool:
      kind: http
      method: POST
      url: "{{ workload.endpoint }}/echo"
      headers: { X-Data: "{{ workload.a }}", X-Plain: "{{ workload.secret }}" }
      body: '{"data": "{{ workload.a }}", "plain": "{{ workload.secret }}"}'
"#,
    );
    let vars = [("endpoint".to_string(), server.uri())].into_iter().collect();
    run(&file, vars);

    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 1);
    let req = &requests[0];
    assert_eq!(req.headers.get("x-data").unwrap().to_str().unwrap(), LITERAL);
    assert_eq!(req.headers.get("x-plain").unwrap().to_str().unwrap(), "S3CRET");
    let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
    assert_eq!(body["data"], LITERAL);
    assert_eq!(body["plain"], "S3CRET");
}

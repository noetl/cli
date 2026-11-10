//! `kind: playbook` on the local runtime hands the child's result to the
//! parent when `return_result: true` (same field as the cluster's
//! noetl-tools `PlaybookConfig`), and stays result-less without it.

use noetl::PlaybookRunner;
use std::path::PathBuf;

const CHILD: &str = r#"
apiVersion: noetl.io/v2
kind: Playbook
metadata:
  name: child
workflow:
  - step: start
    next:
      - step: first
  - step: first
    tool:
      kind: shell
      cmds:
        - echo "first-{{ workload.word }}"
    next:
      - step: last
  - step: last
    tool:
      kind: shell
      cmds:
        - echo '{"word":"{{ workload.word }}","n":3}'
"#;

fn fixture(name: &str, parent: &str) -> (PathBuf, PathBuf) {
    let dir = std::env::temp_dir().join(format!("noetl-pbres-{}-{}", name, std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("child.yaml"), CHILD).unwrap();
    std::fs::write(dir.join("parent.yaml"), parent).unwrap();
    (dir.clone(), dir.join("parent.yaml"))
}

fn run(path: PathBuf) -> anyhow::Result<noetl::RunOutcome> {
    PlaybookRunner::new(path).with_quiet(true).run()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn return_result_hands_child_final_result_to_parent() {
    let (dir, parent) = fixture(
        "final",
        r#"
apiVersion: noetl.io/v2
kind: Playbook
metadata:
  name: parent
workflow:
  - step: start
    next:
      - step: call
  - step: call
    tool:
      kind: playbook
      path: child.yaml
      input:
        word: hi
      return_result: true
    vars:
      n: "{{ result.n }}"
    next:
      - step: use
  - step: use
    tool:
      kind: shell
      cmds:
        - echo "n={{ vars.n }}"
"#,
    );
    let outcome = run(parent).expect("parent runs");
    assert_eq!(outcome.status, "ok");
    let call: serde_json::Value = serde_json::from_str(&outcome.step_results["call"]).unwrap();
    assert_eq!(call["word"], "hi");
    assert_eq!(call["n"], 3);
    // The child's JSON result is addressable through `vars:` like any step.
    assert_eq!(outcome.step_results["use"], "n=3");
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn result_step_selects_a_named_child_step() {
    let (dir, parent) = fixture(
        "named",
        r#"
apiVersion: noetl.io/v2
kind: Playbook
metadata:
  name: parent
workflow:
  - step: start
    next:
      - step: call
  - step: call
    tool:
      kind: playbook
      path: child.yaml
      input:
        word: yo
      return_result: true
      result_step: first
    next:
      - step: use
  - step: use
    tool:
      kind: shell
      cmds:
        - echo "saw {{ call.result }}"
"#,
    );
    let outcome = run(parent).expect("parent runs");
    assert_eq!(outcome.step_results["call"], "first-yo");
    assert_eq!(outcome.step_results["use"], "saw first-yo");
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn without_return_result_the_step_has_no_result() {
    let (dir, parent) = fixture(
        "legacy",
        r#"
apiVersion: noetl.io/v2
kind: Playbook
metadata:
  name: parent
workflow:
  - step: start
    next:
      - step: call
  - step: call
    tool:
      kind: playbook
      path: child.yaml
      input:
        word: zz
"#,
    );
    let outcome = run(parent).expect("parent runs");
    assert_eq!(outcome.status, "ok");
    assert!(
        !outcome.step_results.contains_key("call"),
        "legacy behaviour: no result without return_result, got {:?}",
        outcome.step_results
    );
    assert_eq!(outcome.final_result, None);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unknown_result_step_is_an_error_not_an_empty_result() {
    let (dir, parent) = fixture(
        "missing",
        r#"
apiVersion: noetl.io/v2
kind: Playbook
metadata:
  name: parent
workflow:
  - step: start
    next:
      - step: call
  - step: call
    tool:
      kind: playbook
      path: child.yaml
      input:
        word: q
      return_result: true
      result_step: nope
"#,
    );
    let err = run(parent).expect_err("a typo in result_step must fail the run");
    let msg = format!("{err:#}");
    assert!(msg.contains("result_step `nope`"), "{msg}");
    assert!(msg.contains("first, last"), "lists the steps that did produce results: {msg}");
    std::fs::remove_dir_all(&dir).ok();
}

//! The local runtime refuses a playbook whose steps use a tool kind it cannot
//! run, before any step runs. Before this, such steps were skipped with a
//! stderr note and the run ended `ok` (noetl/ai-meta#539 build plan item 2).

use noetl::PlaybookRunner;

fn write(name: &str, body: &str) -> (std::path::PathBuf, std::path::PathBuf) {
    let dir = std::env::temp_dir().join(format!("noetl-unsupported-{}-{}", name, std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("p.yaml");
    std::fs::write(&path, body).unwrap();
    (dir, path)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mcp_step_fails_before_anything_runs() {
    let (dir, path) = write(
        "mcp",
        r#"
apiVersion: noetl.io/v2
kind: Playbook
metadata: { name: calls_mcp }
workflow:
  - step: start
    next: [ { step: touch } ]
  - step: touch
    tool: { kind: shell, cmds: [ "touch {{ workload.marker }}" ] }
    next: [ { step: read } ]
  - step: read
    tool: { kind: mcp, server: mcp/noetl-cluster, tool: graph.node }
    next: [ { step: also } ]
  - step: also
    tool: { kind: postgres, query: "select 1" }
"#,
    );
    let marker = dir.join("ran");
    let vars = [("marker".to_string(), marker.display().to_string())].into_iter().collect();
    let err = PlaybookRunner::new(path).with_variables(vars).with_quiet(true).run().unwrap_err();
    let msg = format!("{err:#}");
    assert!(msg.contains("step `read`: kind `mcp`"), "{msg}");
    assert!(msg.contains("step `also`: kind `postgres`"), "every offending step is listed: {msg}");
    assert!(msg.contains("--runtime distributed"), "{msg}");
    assert!(!marker.exists(), "refused at load: the shell step before it must not have run");
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_miscased_kind_is_unsupported_not_skipped() {
    let (dir, path) = write(
        "case",
        "apiVersion: noetl.io/v2\nkind: Playbook\nmetadata: { name: c }\nworkflow:\n  - step: s\n    tool: { kind: Shell, cmds: [ 'true' ] }\n",
    );
    let msg = format!("{:#}", PlaybookRunner::new(path).with_quiet(true).run().unwrap_err());
    assert!(msg.contains("kind `Shell`"), "{msg}");
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn noop_is_still_a_no_op() {
    let (dir, path) = write(
        "noop",
        r#"
apiVersion: noetl.io/v2
kind: Playbook
metadata: { name: with_noop }
workflow:
  - step: start
    tool: { kind: noop }
    next: [ { step: say } ]
  - step: say
    tool: { kind: shell, cmds: [ "echo hi" ] }
    next: [ { step: end } ]
  - step: end
    tool: { kind: noop }
"#,
    );
    let outcome = PlaybookRunner::new(path).with_quiet(true).run().expect("noop steps run");
    assert_eq!(outcome.status, "ok");
    assert_eq!(outcome.step_results["say"], "hi");
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unsupported_kind_in_a_sub_playbook_fails_the_parent() {
    let dir = std::env::temp_dir().join(format!("noetl-unsupported-sub-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("child.yaml"),
        "apiVersion: noetl.io/v2\nkind: Playbook\nmetadata: { name: child }\nworkflow:\n  - step: s\n    tool: { kind: workbook, name: t }\n",
    )
    .unwrap();
    std::fs::write(
        dir.join("parent.yaml"),
        "apiVersion: noetl.io/v2\nkind: Playbook\nmetadata: { name: parent }\nworkflow:\n  - step: call\n    tool: { kind: playbook, path: child.yaml }\n",
    )
    .unwrap();
    let msg = format!("{:#}", PlaybookRunner::new(dir.join("parent.yaml")).with_quiet(true).run().unwrap_err());
    assert!(msg.contains("kind `workbook`"), "{msg}");
    std::fs::remove_dir_all(&dir).ok();
}

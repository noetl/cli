//! End-to-end check of `kind: agent` in the local runtime: the runner drives
//! the model loop against a mock Messages API and answers a tool call by
//! running the bound sub-playbook.

use noetl::PlaybookRunner;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

const PARENT: &str = r#"
apiVersion: noetl.io/v2
kind: Playbook
metadata:
  name: agent_e2e
workflow:
  - step: start
    next:
      - step: ask
  - step: ask
    tool:
      kind: agent
      framework: claude
      model: claude-test
      prompt: "Echo something"
      endpoint: "{{ workload.endpoint }}"
      max_turns: 3
      auth:
        type: api_key
        credential: agent-e2e
      tools:
        - name: echo
          description: Echo a word.
          input_schema:
            type: object
            properties:
              word: { type: string }
          playbook: echo.yaml
"#;

const ECHO: &str = r#"
apiVersion: noetl.io/v2
kind: Playbook
metadata:
  name: echo
workflow:
  - step: start
    next:
      - step: say
  - step: say
    tool:
      kind: shell
      cmds:
        - echo "echoed {{ workload.word }}"
"#;

/// Text of the tool_result the runner sent back, if this request carries one.
fn tool_result_text(req: &Request) -> Option<String> {
    let body: serde_json::Value = serde_json::from_slice(&req.body).ok()?;
    let last = body["messages"].as_array()?.last()?.clone();
    let block = last["content"].as_array()?.first()?.clone();
    (block["type"] == "tool_result").then(|| block["content"].as_str().unwrap_or("").to_string())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn agent_step_runs_tool_playbook_and_returns_final_text() {
    let server = MockServer::start().await;

    // Turn 2: only answered once the runner has sent the sub-playbook output.
    // The model input `{{ workload.endpoint }}` must come back defanged, not
    // expanded to the endpoint URL by the sub-playbook renderer.
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .and(|req: &Request| tool_result_text(req).is_some_and(|t| t.contains("echoed { { workload.endpoint }}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "content": [{ "type": "text", "text": "all done" }],
            "stop_reason": "end_turn",
            "usage": { "input_tokens": 20, "output_tokens": 4 },
        })))
        .expect(1)
        .mount(&server)
        .await;
    // Turn 1: ask for the tool.
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .and(header("x-api-key", "e2e-key"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "content": [{
                "type": "tool_use", "id": "tu_1", "name": "echo",
                "input": { "word": "{{ workload.endpoint }}" },
            }],
            "stop_reason": "tool_use",
            "usage": { "input_tokens": 10, "output_tokens": 2 },
        })))
        .up_to_n_times(1)
        .expect(1)
        .mount(&server)
        .await;

    let dir = std::env::temp_dir().join(format!("noetl-agent-e2e-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("parent.yaml"), PARENT).unwrap();
    std::fs::write(dir.join("echo.yaml"), ECHO).unwrap();

    // The only test in this binary that touches the variable.
    std::env::set_var("NOETL_CREDENTIAL_AGENT_E2E", "e2e-key");

    let vars = [("endpoint".to_string(), server.uri())].into_iter().collect();
    let outcome = PlaybookRunner::new(dir.join("parent.yaml"))
        .with_variables(vars)
        .with_quiet(true)
        .run();
    let outcome = match outcome {
        Ok(o) => o,
        Err(e) => {
            let sent: Vec<Option<String>> = server
                .received_requests()
                .await
                .unwrap_or_default()
                .iter()
                .map(tool_result_text)
                .collect();
            panic!("agent playbook failed: {e}\ntool_result texts sent: {sent:?}");
        }
    };

    assert_eq!(outcome.status, "ok", "{:?}", outcome.error);
    let result: serde_json::Value = serde_json::from_str(outcome.step_results.get("ask").expect("ask result")).unwrap();
    assert_eq!(result["text"], "all done");
    assert_eq!(result["turns"], 2);
    assert_eq!(result["usage"]["input_tokens"], 30);

    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn claude_code_step_runs_the_cli_without_auth() {
    use std::os::unix::fs::PermissionsExt;

    let dir = std::env::temp_dir().join(format!("noetl-agent-cc-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    // Fake `claude`: echoes stdin and argv into a Claude Code JSON result.
    let bin = dir.join("claude");
    std::fs::write(
        &bin,
        "#!/bin/sh\nprompt=$(cat)\nprintf '{\"type\":\"result\",\"is_error\":false,\"num_turns\":1,\
         \"result\":\"prompt=%s args=%s\",\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}' \"$prompt\" \"$*\"\n",
    )
    .unwrap();
    std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();
    std::fs::write(
        dir.join("cc.yaml"),
        r#"
apiVersion: noetl.io/v2
kind: Playbook
metadata:
  name: agent_cc
workload:
  who: world
workflow:
  - step: start
    next:
      - step: ask
  - step: ask
    tool:
      kind: agent
      framework: claude-code
      model: claude-test
      prompt: "hello {{ workload.who }}"
      claude_bin: "{{ workload.bin }}"
      allowed_tools: [Read]
"#,
    )
    .unwrap();

    let vars = [("bin".to_string(), bin.to_string_lossy().to_string())]
        .into_iter()
        .collect();
    let outcome = PlaybookRunner::new(dir.join("cc.yaml"))
        .with_variables(vars)
        .with_quiet(true)
        .run()
        .expect("claude-code playbook runs");
    assert_eq!(outcome.status, "ok", "{:?}", outcome.error);
    let result: serde_json::Value = serde_json::from_str(&outcome.step_results["ask"]).unwrap();
    let text = result["text"].as_str().unwrap();
    assert!(text.contains("prompt=hello world"), "{text}");
    assert!(text.contains("--model claude-test") && text.contains("--allowed-tools Read"), "{text}");
    assert_eq!(result["framework"], "claude-code");

    std::fs::remove_dir_all(&dir).ok();
}

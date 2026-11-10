//! Outbound MCP and external agents on the local runtime (build plan items 3
//! and 6): `kind: mcp` with a credential alias from the local store, an agent
//! whose tool is an MCP server, and `framework: remote` calling an external
//! agent endpoint.  All three dispatch through noetl-tools, the code the
//! cluster worker runs.

use noetl::PlaybookRunner;
use serde_json::{json, Value};
use wiremock::matchers::{body_partial_json, header, method, path};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

fn write(name: &str, body: &str) -> (std::path::PathBuf, std::path::PathBuf) {
    let dir = std::env::temp_dir().join(format!("noetl-mcp-local-{}-{}", name, std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("p.yaml");
    std::fs::write(&path, body).unwrap();
    (dir, path)
}

async fn mcp_server(bearer: &str) -> MockServer {
    let s = MockServer::start().await;
    let auth = format!("Bearer {bearer}");
    for (m, result) in [
        (
            "initialize",
            json!({"protocolVersion": "2025-03-26", "capabilities": {}}),
        ),
        (
            "tools/list",
            json!({"tools": [{"name": "graph.node", "description": "Read a node.",
                              "inputSchema": {"type": "object", "properties": {"id": {"type": "string"}}}}]}),
        ),
        (
            "tools/call",
            json!({"content": [{"type": "text", "text": "r1 is a router"}], "isError": false}),
        ),
    ] {
        Mock::given(method("POST"))
            .and(path("/mcp"))
            .and(header("authorization", auth.as_str()))
            .and(body_partial_json(json!({"method": m})))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(json!({"jsonrpc": "2.0", "id": 1, "result": result})),
            )
            .mount(&s)
            .await;
    }
    Mock::given(method("POST"))
        .and(path("/mcp"))
        .and(body_partial_json(json!({"method": "notifications/initialized"})))
        .respond_with(ResponseTemplate::new(202))
        .mount(&s)
        .await;
    s
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mcp_step_calls_a_server_with_a_local_credential() {
    let mcp = mcp_server("local-mcp-token").await;
    std::env::set_var("NOETL_CREDENTIAL_MCP_LOCAL_TEST", "local-mcp-token");
    let (dir, p) = write(
        "step",
        r#"
apiVersion: noetl.io/v2
kind: Playbook
metadata: { name: calls_mcp }
workflow:
  - step: start
    next: [ { step: read } ]
  - step: read
    tool:
      kind: mcp
      endpoint: "{{ workload.mcp }}/mcp"
      tool: graph.node
      arguments: { id: r1 }
      auth: mcp_local_test
      on_provider_error: fail
"#,
    );
    let vars = [("mcp".to_string(), mcp.uri())].into_iter().collect();
    let out = PlaybookRunner::new(p)
        .with_variables(vars)
        .with_quiet(true)
        .run()
        .expect("mcp playbook runs");
    assert_eq!(out.status, "ok", "{:?}", out.error);
    let r: Value = serde_json::from_str(&out.step_results["read"]).unwrap();
    assert_eq!(r["status"], "ok", "{r}");
    assert_eq!(r["text"], "r1 is a router");
    assert_eq!(r["sent_headers"], json!(["authorization"]));
    assert!(!out.step_results["read"].contains("local-mcp-token"));
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mcp_step_fails_the_run_when_the_server_refuses_and_policy_is_fail() {
    let mcp = mcp_server("right").await;
    std::env::set_var("NOETL_CREDENTIAL_MCP_LOCAL_WRONG", "wrong");
    let (dir, p) = write(
        "refused",
        r#"
apiVersion: noetl.io/v2
kind: Playbook
metadata: { name: refused }
workflow:
  - step: start
    next: [ { step: read } ]
  - step: read
    tool: { kind: mcp, endpoint: "{{ workload.mcp }}/mcp", method: tools/list, auth: mcp_local_wrong, on_provider_error: fail }
"#,
    );
    let vars = [("mcp".to_string(), mcp.uri())].into_iter().collect();
    let res = PlaybookRunner::new(p).with_variables(vars).with_quiet(true).run();
    let failed = match res {
        Err(_) => true,
        Ok(o) => o.status != "ok",
    };
    assert!(
        failed,
        "a refused MCP call with on_provider_error: fail must fail the run"
    );
    std::fs::remove_dir_all(&dir).ok();
}

fn tool_result(req: &Request) -> Option<String> {
    let body: Value = serde_json::from_slice(&req.body).ok()?;
    let last = body["messages"].as_array()?.last()?.clone();
    let block = last["content"].as_array()?.first()?.clone();
    (block["type"] == "tool_result").then(|| block["content"].as_str().unwrap_or("").to_string())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn agent_calls_an_mcp_server_tool() {
    let mcp = mcp_server("agent-mcp-token").await;
    let llm = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .and(|r: &Request| tool_result(r).is_some_and(|t| t == "r1 is a router"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "content": [{"type": "text", "text": "r1 is a router, per the graph"}],
            "stop_reason": "end_turn", "usage": {"input_tokens": 9, "output_tokens": 3}
        })))
        .expect(1)
        .mount(&llm)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .and(header("x-api-key", "agent-key"))
        .and(|r: &Request| {
            let b: Value = serde_json::from_slice(&r.body).unwrap();
            b["tools"][0]["name"] == "graph__graph_node"
        })
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "content": [{"type": "tool_use", "id": "tu_1", "name": "graph__graph_node", "input": {"id": "r1"}}],
            "stop_reason": "tool_use", "usage": {"input_tokens": 5, "output_tokens": 2}
        })))
        .up_to_n_times(1)
        .expect(1)
        .mount(&llm)
        .await;
    std::env::set_var("NOETL_CREDENTIAL_AGENT_LOCAL_KEY", "agent-key");
    std::env::set_var("NOETL_CREDENTIAL_AGENT_LOCAL_MCP", "agent-mcp-token");
    let (dir, p) = write(
        "agent",
        r#"
apiVersion: noetl.io/v2
kind: Playbook
metadata: { name: agent_mcp }
workflow:
  - step: start
    next: [ { step: ask } ]
  - step: ask
    tool:
      kind: agent
      framework: claude
      model: claude-test
      prompt: What is r1?
      endpoint: "{{ workload.llm }}"
      auth: { type: api_key, credential: agent_local_key }
      mcp_servers:
        - name: graph
          endpoint: "{{ workload.mcp }}/mcp"
          auth: { type: bearer, credential: agent_local_mcp }
"#,
    );
    let vars = [("llm".to_string(), llm.uri()), ("mcp".to_string(), mcp.uri())]
        .into_iter()
        .collect();
    let out = PlaybookRunner::new(p)
        .with_variables(vars)
        .with_quiet(true)
        .run()
        .expect("agent runs");
    assert_eq!(out.status, "ok", "{:?}", out.error);
    let r: Value = serde_json::from_str(&out.step_results["ask"]).unwrap();
    assert_eq!(r["text"], "r1 is a router, per the graph");
    assert_eq!(r["source_tool_calls"], 1);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn framework_remote_calls_an_external_agent() {
    let peer = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/agents/send"))
        .and(body_partial_json(
            json!({"agent": "site/ask", "input": {"question": "status?"}}),
        ))
        .respond_with(ResponseTemplate::new(202).set_body_json(json!({"task_id": "7", "status": "submitted"})))
        .mount(&peer)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/agents/tasks/7"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "task_id": "7", "status": "completed", "result": {"answer": "all links up"}
        })))
        .mount(&peer)
        .await;
    let (dir, p) = write(
        "remote",
        r#"
apiVersion: noetl.io/v2
kind: Playbook
metadata: { name: remote_agent }
workflow:
  - step: start
    next: [ { step: ask } ]
  - step: ask
    tool:
      kind: agent
      framework: remote
      remote: { endpoint: "{{ workload.peer }}", protocol: noetl, agent: site/ask }
      input: { question: "status?" }
"#,
    );
    let vars = [("peer".to_string(), peer.uri())].into_iter().collect();
    let out = PlaybookRunner::new(p)
        .with_variables(vars)
        .with_quiet(true)
        .run()
        .expect("remote agent runs");
    assert_eq!(out.status, "ok", "{:?}", out.error);
    let r: Value = serde_json::from_str(&out.step_results["ask"]).unwrap();
    assert_eq!(r["result"]["answer"], "all links up", "{r}");
    assert_eq!(r["framework"], "remote");
    std::fs::remove_dir_all(&dir).ok();
}

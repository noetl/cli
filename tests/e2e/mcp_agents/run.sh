#!/usr/bin/env bash
# MCP + agents across the local runtime and the daemon: the four directions,
# each on `noetl run` (local) and `noetl subscribe` (daemon), with real
# processes on loopback only.
#
#   NOETL=/path/to/noetl tests/e2e/mcp_agents/run.sh
#
# Topology (all 127.0.0.1):
#   serve   :18765  `noetl serve` — local runtime serving site/ask (MCP tool + agent)
#   daemon  :18766  `noetl subscribe --serve` — timer daemon serving the same playbook
#   model   :18767  stub Anthropic Messages API (scripted: call every tool, then echo)
#   a2a     :18768  stub A2A JSON-RPC agent
#   cb      :18769  callback receiver
# The local caller targets the daemon and the daemon's tick targets `noetl serve`,
# so each direction also crosses runtimes. Needs: noetl (this branch), python3, curl, jq.
set -uo pipefail
NOETL=${NOETL:-noetl}
HERE=$(cd "$(dirname "$0")" && pwd)
W=$(mktemp -d)
P_SERVE=${P_SERVE:-18765} P_DAEMON=${P_DAEMON:-18766} P_MODEL=${P_MODEL:-18767} P_A2A=${P_A2A:-18768} P_CB=${P_CB:-18769}
L=127.0.0.1
PIDS=()
pass=0 fail=0
cleanup() { for p in "${PIDS[@]}"; do kill "$p" 2>/dev/null; done; wait 2>/dev/null; [ -n "${KEEP:-}" ] || rm -rf "$W"; }
trap cleanup EXIT
ok() { pass=$((pass+1)); echo "  ✔ $1"; }
ko() { fail=$((fail+1)); echo "  ✘ $1"; [ -n "${2:-}" ] && echo "      $2" | head -c 600 && echo; }
check() { if echo "$2" | grep -qF -- "$3"; then ok "$1"; else ko "$1" "$2"; fi; }
up() { for _ in $(seq 1 100); do curl -sf "http://$L:$1/healthz" >/dev/null && return 0; sleep 0.2; done; return 1; }
rpc() { curl -s "http://$L:$1/mcp" -H 'content-type: application/json' -d "$2"; }

python3 -I "$HERE/stubs.py" model "$P_MODEL" & PIDS+=($!)
python3 -I "$HERE/stubs.py" a2a "$P_A2A" & PIDS+=($!)
python3 -I "$HERE/stubs.py" callback "$P_CB" "$W/callbacks.jsonl" & PIDS+=($!)

NOETL_E2E_WHO=serve "$NOETL" serve --playbooks "$HERE/served" --listen "$L:$P_SERVE" \
  --callback-origin "http://$L:$P_CB" >"$W/serve.log" 2>&1 & PIDS+=($!)

sed -e "s#@MCP@#http://$L:$P_SERVE/mcp#" -e "s#@PEER@#http://$L:$P_SERVE#" \
    -e "s#@A2A@#http://$L:$P_A2A#" -e "s#@MODEL@#http://$L:$P_MODEL#" \
    "$HERE/callers/daemon.yaml" > "$W/daemon.yaml"
NOETL_E2E_OUT="$W/tick.txt" NOETL_CREDENTIAL_E2E_MODEL=stub-key NOETL_E2E_WHO=daemon "$NOETL" subscribe "$W/daemon.yaml" \
  --playbook-dir "$HERE/callers" --events "$W/events.jsonl" \
  --serve "$L:$P_DAEMON" --serve-playbooks "$HERE/served" --callback-origin "http://$L:$P_CB" \
  --verbose >"$W/daemon.log" 2>&1 & PIDS+=($!)

up "$P_SERVE" || { echo "noetl serve did not come up"; cat "$W/serve.log"; exit 1; }
up "$P_DAEMON" || { echo "daemon --serve did not come up"; cat "$W/daemon.log"; exit 1; }

echo "== 3. a playbook IS an MCP server"
for rt in serve:$P_SERVE daemon:$P_DAEMON; do
  name=${rt%%:*} port=${rt##*:}
  list=$(rpc "$port" '{"jsonrpc":"2.0","id":1,"method":"tools/list"}')
  check "$name: tools/list has site_status" "$list" '"name":"site_status"'
  call=$(rpc "$port" '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"site_status","arguments":{"question":"q3"}}}')
  check "$name: tools/call runs the playbook" "$call" "site ok (q3) via $name"
  bad=$(rpc "$port" '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"site_status","arguments":{"question":"q","workload_override":1}}}')
  check "$name: undeclared argument refused" "$bad" '"isError":true'
done

echo "== 4. a playbook IS an agent (card, send, poll, callback)"
for rt in serve:$P_SERVE daemon:$P_DAEMON; do
  name=${rt%%:*} port=${rt##*:}
  card=$(curl -s "http://$L:$port/api/agents/cards/site/ask")
  check "$name: agent card" "$card" '"name":"site-asker"'
  sent=$(curl -s "http://$L:$port/api/agents/send" -H 'content-type: application/json' \
    -d "{\"agent\":\"site/ask\",\"input\":{\"question\":\"q4-$name\"},\"callback_url\":\"http://$L:$P_CB/cb\"}")
  tid=$(echo "$sent" | jq -r .task_id) tok=$(echo "$sent" | jq -r .callback_token)
  view=""
  for _ in $(seq 1 100); do view=$(curl -s "http://$L:$port/api/agents/tasks/$tid"); echo "$view" | grep -q '"working"' || break; sleep 0.2; done
  check "$name: task completed with the playbook's result" "$view" "site ok (q4-$name) via $name"
  for _ in $(seq 1 50); do grep -q "q4-$name" "$W/callbacks.jsonl" 2>/dev/null && break; sleep 0.2; done
  cb=$(grep "q4-$name" "$W/callbacks.jsonl" 2>/dev/null | head -1)
  check "$name: callback delivered with the task's own token" "$cb" "\"authorization\": \"Bearer $tok\""
  refused=$(curl -s -o /dev/null -w '%{http_code}' "http://$L:$port/api/agents/send" -H 'content-type: application/json' \
    -d '{"agent":"site/ask","input":{"question":"x"},"callback_url":"http://169.254.169.254/latest"}')
  check "$name: callback to an unlisted origin refused" "$refused" "403"
done

echo "== 1 + 2 on the local runtime (noetl run; targets the daemon)"
runj() { "$NOETL" run "$HERE/callers/$1" -r local --json "${@:2}" 2>"$W/$1.err"; }
out=$(runj d1_mcp.yaml --set mcp="http://$L:$P_DAEMON/mcp")
check "local: kind: mcp calls the daemon's MCP server" "$out" "site ok (d1) via daemon"
out=$(runj d2_remote_noetl.yaml --set peer="http://$L:$P_DAEMON")
check "local: framework remote (noetl) calls the daemon's agent" "$out" "site ok (d2-noetl) via daemon"
out=$(runj d2_remote_a2a.yaml --set a2a="http://$L:$P_A2A")
check "local: framework remote (a2a)" "$out" "a2a says: d2-a2a"
out=$(NOETL_CREDENTIAL_E2E_MODEL=stub-key runj d2_agent_sources.yaml --set model="http://$L:$P_MODEL" \
  --set mcp="http://$L:$P_DAEMON/mcp" --set peer="http://$L:$P_DAEMON")
check "local: agent's MCP tool source answered" "$out" "site ok (from-model) via daemon | site ok (from-model) via daemon"
check "local: agent's remote-agent tool source answered" "$out" "| site ok (from-model) via daemon"
check "local: two source calls counted" "$out" 'source_tool_calls\":2'

echo "== 1 + 2 on the daemon (timer tick; targets noetl serve)"
# The daemon's JSONL trail records playbook start/finish; the tick's last step
# writes its step results to $NOETL_E2E_OUT.
for _ in $(seq 1 150); do grep -q 'playbook.completed' "$W/events.jsonl" 2>/dev/null && [ -s "$W/tick.txt" ] && break; sleep 0.2; done
ev=$(cat "$W/tick.txt" 2>/dev/null)
check "daemon: the tick playbook completed without error" "$(grep playbook.completed "$W/events.jsonl" | head -1)" '"error":null'
check "daemon: kind: mcp calls noetl serve" "$ev" "site ok (d1) via serve"
check "daemon: framework remote (noetl) calls noetl serve's agent" "$ev" "site ok (d2-noetl) via serve"
check "daemon: framework remote (a2a)" "$ev" "a2a says: d2-a2a"
check "daemon: agent with MCP + agent tool sources" "$ev" "model saw: site ok (from-model) via serve"

echo
echo "passed=$pass failed=$fail"
[ "$fail" -eq 0 ]

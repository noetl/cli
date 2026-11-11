#!/usr/bin/env bash
# Local e2e: `noetl serve --task-db` across a real process kill (SIGKILL, no
# graceful shutdown).
#
#   1  serve with --task-db; a quick task completes
#   2  a slow task is accepted, then the process is SIGKILLed mid-run
#   3  a new process on the same file: the quick task still answers; the slow
#      one is rerun and completes; an owed callback is delivered
#   4  control: the same kill without --task-db loses the task (404)
#
# Usage: NOETL_BIN=target/debug/noetl ci/serve_restart_e2e.sh
set -euo pipefail
BIN=${NOETL_BIN:-target/debug/noetl}
[ -x "$BIN" ] || { echo "REFUSED: $BIN not built"; exit 2; }
W=$(mktemp -d "${TMPDIR:-/tmp}/serve-restart.XXXXXX")
PORT=${PORT:-38765}; CBPORT=$((PORT + 1))
B=http://127.0.0.1:$PORT
pass=0; fails=(); PID=; CBPID=
ok()   { pass=$((pass+1)); printf '  ok   %s\n' "$*"; }
bad()  { fails+=("$*"); printf '  FAIL %s\n' "$*"; }
step() { printf '\n== %s\n' "$*"; }
cleanup() {
  [ -n "$PID" ] && kill -9 "$PID" 2>/dev/null || true
  [ -n "$CBPID" ] && kill "$CBPID" 2>/dev/null || true
  echo; echo "passed: $pass  failed: ${#fails[@]}"
  for f in ${fails[@]+"${fails[@]}"}; do echo "  - $f"; done
}
trap cleanup EXIT

mkdir -p "$W/pb"
cat > "$W/pb/quick.yaml" <<'YAML'
apiVersion: noetl.io/v2
kind: Playbook
metadata:
  name: quick
  path: demo/quick
  expose:
    agent:
      card: { name: quick, description: answers at once }
      input_schema: { type: object, properties: { q: { type: string } } }
workflow:
  - step: start
    next: [ { step: say } ]
  - step: say
    tool: { kind: shell, cmds: [ "echo 'quick: {{ workload.q }}'" ] }
YAML
cat > "$W/pb/slow.yaml" <<'YAML'
apiVersion: noetl.io/v2
kind: Playbook
metadata:
  name: slow
  path: demo/slow
  expose:
    agent:
      card: { name: slow, description: takes a few seconds }
      input_schema: { type: object, properties: { q: { type: string } } }
workflow:
  - step: start
    next: [ { step: work } ]
  - step: work
    tool: { kind: shell, cmds: [ "sleep 3", "echo 'slow: {{ workload.q }}'" ] }
YAML

# A callback receiver that records every POST body.
cat > "$W/cb.py" <<'PY'
import http.server, sys
out = sys.argv[2]
class H(http.server.BaseHTTPRequestHandler):
    def log_message(self, *a): pass
    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("content-length", 0)))
        with open(out, "ab") as f: f.write(body + b"\n")
        self.send_response(200); self.end_headers()
http.server.HTTPServer(("127.0.0.1", int(sys.argv[1])), H).serve_forever()
PY
python3 "$W/cb.py" "$CBPORT" "$W/callbacks.jsonl" & CBPID=$!

serve() { # serve [extra args...]
  "$BIN" serve --playbooks "$W/pb" --listen "127.0.0.1:$PORT" --no-mcp \
    --callback-origin "http://127.0.0.1:$CBPORT" "$@" > "$W/serve.log" 2>&1 & PID=$!
  local i=0; until curl -fsS -m 1 "$B/healthz" >/dev/null 2>&1; do i=$((i+1)); [ $i -lt 50 ] || return 1; sleep 0.2; done
}
send() { curl -sS -m 5 -X POST "$B/api/agents/send" -H 'content-type: application/json' -d "$1"; }
task() { curl -sS -m 5 -o "$W/task.json" -w '%{http_code}' "$B/api/agents/tasks/$1"; }
wait_task() { local i=0 st; while :; do task "$1" >/dev/null; st=$(jq -r .status "$W/task.json"); [ "$st" != working ] && break; i=$((i+1)); [ $i -lt 60 ] || break; sleep 0.25; done; cat "$W/task.json"; }

step "1. durable serve: a quick task completes"
serve --task-db "$W/state/tasks.db" || { bad "serve did not start: $(tail -3 "$W/serve.log")"; exit 1; }
grep -q "durable in" "$W/serve.log" && ok "serve reports the durable store: $(grep 'durable in' "$W/serve.log" | sed 's/^ *//')" || bad "no durable line in serve log"
Q=$(send '{"agent":"demo/quick","input":{"q":"one"}}' | jq -r .task_id)
[ "$(wait_task "$Q" | jq -r .result)" = "quick: one" ] && ok "quick task $Q completed" || bad "quick task: $(cat "$W/task.json")"

step "2. SIGKILL during a slow task"
S=$(send "{\"agent\":\"demo/slow\",\"input\":{\"q\":\"two\"},\"callback_url\":\"http://127.0.0.1:$CBPORT/cb\"}" | jq -r .task_id)
sleep 1
task "$S" >/dev/null; [ "$(jq -r .status "$W/task.json")" = working ] && ok "slow task $S is working" || bad "slow task not working: $(cat "$W/task.json")"
kill -9 "$PID"; wait "$PID" 2>/dev/null || true; PID=
ok "serve SIGKILLed mid-run"
[ ! -s "$W/callbacks.jsonl" ] && ok "no callback was delivered before the kill" || bad "callback before kill"

step "3. restart on the same file"
serve --task-db "$W/state/tasks.db" || { bad "restart failed: $(tail -3 "$W/serve.log")"; exit 1; }
grep -q "(1 resumed" "$W/serve.log" && ok "restart: $(grep 'durable in' "$W/serve.log" | sed 's/^ *//')" || bad "resume line: $(grep 'tasks' "$W/serve.log")"
[ "$(task "$Q")" = 200 ] && [ "$(jq -r .result "$W/task.json")" = "quick: one" ] && ok "the quick task finished before the kill still answers" || bad "quick after restart: $(cat "$W/task.json")"
[ "$(wait_task "$S" | jq -r .result)" = "slow: two" ] && ok "the slow task was rerun after the restart and completed" || bad "slow after restart: $(cat "$W/task.json")"
i=0; until [ -s "$W/callbacks.jsonl" ] || [ $i -ge 40 ]; do i=$((i+1)); sleep 0.25; done
[ "$(wc -l < "$W/callbacks.jsonl" | tr -d ' ')" = 1 ] && [ "$(jq -r .task_id "$W/callbacks.jsonl")" = "$S" ] \
  && ok "its callback was delivered once, after the restart" || bad "callbacks: $(cat "$W/callbacks.jsonl" 2>/dev/null)"
[ "$(python3 -c "import os,sys; print(oct(os.stat(sys.argv[1]).st_mode & 0o777)[2:])" "$W/state/tasks.db")" = 600 ] && ok "task db is mode 0600" || bad "task db mode"
kill -9 "$PID"; wait "$PID" 2>/dev/null || true; PID=

step "4. control: the default in-memory store"
serve || { bad "memory serve did not start"; exit 1; }
M=$(send '{"agent":"demo/slow","input":{"q":"three"}}' | jq -r .task_id)
sleep 1; kill -9 "$PID"; wait "$PID" 2>/dev/null || true; PID=
serve || { bad "memory restart failed"; exit 1; }
[ "$(task "$M")" = 404 ] && ok "without --task-db the task is gone after a restart (404), as before" || bad "memory control: $(cat "$W/task.json")"

[ ${#fails[@]} -eq 0 ]

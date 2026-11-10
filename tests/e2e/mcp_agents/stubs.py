"""Loopback stubs for tests/e2e/mcp_agents/run.sh.

  python3 -I stubs.py model    PORT   # Anthropic Messages API: turn 1 calls tool[0], turn 2 echoes the tool_result
  python3 -I stubs.py a2a      PORT   # A2A JSON-RPC agent: message/send -> working task, tasks/get -> completed
  python3 -I stubs.py callback PORT FILE  # records every POST (headers + body) as one JSON line
"""
import json, sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

mode, port = sys.argv[1], int(sys.argv[2])
out = sys.argv[3] if len(sys.argv) > 3 else None
tasks = {}

class H(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def reply(self, code, body):
        data = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_POST(self):
        raw = self.rfile.read(int(self.headers.get("content-length", 0)))
        body = json.loads(raw or b"{}")
        if mode == "model":
            last = body["messages"][-1]
            content = last["content"]
            if isinstance(content, list) and content and content[0].get("type") == "tool_result":
                texts = [b.get("content", "") for b in content]
                return self.reply(200, {"content": [{"type": "text", "text": "model saw: " + " | ".join(texts)}],
                                        "stop_reason": "end_turn", "usage": {"input_tokens": 1, "output_tokens": 1}})
            tools = body.get("tools", [])
            calls = []
            for i, t in enumerate(tools):
                props = (t.get("input_schema") or {}).get("properties", {})
                arg = {k: "from-model" for k in props}
                calls.append({"type": "tool_use", "id": f"tu_{i}", "name": t["name"], "input": arg})
            return self.reply(200, {"content": calls, "stop_reason": "tool_use",
                                    "usage": {"input_tokens": 1, "output_tokens": 1}})
        if mode == "a2a":
            m, p = body.get("method"), body.get("params", {})
            if m == "message/send":
                text = p["message"]["parts"][0]["text"]
                tid = f"t-{len(tasks) + 1}"
                tasks[tid] = text
                return self.reply(200, {"jsonrpc": "2.0", "id": body.get("id"),
                                        "result": {"kind": "task", "id": tid, "status": {"state": "working"}}})
            if m == "tasks/get":
                tid = p["id"]
                return self.reply(200, {"jsonrpc": "2.0", "id": body.get("id"),
                                        "result": {"kind": "task", "id": tid, "status": {"state": "completed"},
                                                   "artifacts": [{"parts": [{"kind": "text", "text": "a2a says: " + tasks[tid]}]}]}})
            return self.reply(200, {"jsonrpc": "2.0", "id": body.get("id"), "error": {"code": -32601, "message": "nope"}})
        if mode == "callback":
            with open(out, "a") as f:
                f.write(json.dumps({"authorization": self.headers.get("authorization", ""), "body": body}) + "\n")
            return self.reply(200, {"ok": True})

ThreadingHTTPServer(("127.0.0.1", port), H).serve_forever()

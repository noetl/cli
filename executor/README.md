# noetl-executor

Shared utilities and types for the NoETL CLI (local-mode runner) and
the NoETL worker (NATS pull consumer) — the architectural hinge
introduced in Appendix H of the global hybrid cloud blueprint.

## What this crate is

A **utilities-and-types** crate.  See [§ H.10 of the blueprint][h10]
for the architectural rationale: the CLI is a recursive tree walker,
the worker is a pull-model consumer, and these two control loops are
fundamentally different shapes.  Flattening either into the other
produces more abstraction than it removes.

Both binaries call into `noetl-executor` for the **same**:

- YAML playbook types (`playbook`)
- Template rendering (`template`)
- Condition evaluation (`condition`)
- Capability validation (`capabilities`)
- Event-emission envelope shape (`events`)
- Credential resolution trait (`runtime`)
- Tool dispatch bridge onto the `noetl-tools` registry (`tools_bridge`)

But each binary keeps its own:

- **CLI**: the recursive tree walker (`repos/cli/src/playbook_runner.rs`).
  Loads the YAML, walks the workflow, evaluates `next` arcs / `case`
  conditions / `then` blocks in place, dispatches each step inline.
  Control flow is the call stack.
- **Worker**: the NATS pull loop (`repos/worker/src/`).  Subscribes to
  a durable consumer, pulls one command at a time, executes it, emits
  events, repeats.  No tree.  No recursion.

## Module layout

| Module | Purpose |
|---|---|
| `playbook` | Pydantic-like YAML types: `Playbook`, `Step`, `Tool`, `NextFormat`, `RuntimeCapabilities`. |
| `template` | `render_template`, `render_template_with_result`, `get_json_path`, `json_to_rhai`, `rhai_to_json_string`. |
| `condition` | `evaluate_condition` (simple `{{ a == b }}` / `'in'` / truthy) and `evaluate_rhai_condition` (full Rhai expression eval). |
| `capabilities` | `validate_capabilities` returning `ValidationReport` + `ValidationError`.  Pure function — returns the report rather than `bail!`ing so each binary can format errors its own way. |
| `runtime` | `ExecutionContext` (executor-side variant with async `step_results` + `Arc<dyn CredentialResolver>`); `CredentialResolver` trait. |
| `events` | `ExecutorEvent` (mirrors the Python `noetl.runtime.events.report_event` envelope), `EventSink` trait, `NoopSink`, `EventEmitter`. |
| `tools_bridge` | Adapter layer between the CLI's YAML-parsed `Tool` enum and the `noetl-tools` registry.  Wires `Tool::Rhai` / `Tool::Shell` / `Tool::Http` / `Tool::DuckDb` through the registry; `Tool::Playbook` / `Tool::Auth` / `Tool::Sink` stay inline per § H.10 with pure helpers (`prepare_sub_playbook_vars`, `resolve_auth_to_bearer`, `auth_context_updates`, `format_sink_payload`, `json_to_csv`). |
| `worker::source` | Worker-only — `Command` envelope + `CommandSource` trait.  CLI's tree walker does NOT consume this. |

## R-1.1 PR landing history

See the [executor-crate-architecture wiki page][wiki] for the full
sub-PR landing-history table and semantic-divergence records.

## Stability

`0.1.x` is pre-production.  Public API churns through R-1.1's sub-PRs
and stabilises around R-1.3 when the worker depends on the crate.
Treat as internal until then; the crate ships with `publish = false`.

## Tests

Run the workspace tests:

```bash
cargo test --workspace
```

- **80 unit tests** under `executor/src/*` cover each module's
  individual surface.
- **End-to-end integration tests** under `executor/tests/dispatch.rs`
  exercise `dispatch_via_registry` against real tools (Rhai, Shell,
  DuckDB) so the seams between the bridge and `noetl-tools` are
  covered too.
- **CLI binary tests** (41 per binary × 2 = 82) cover
  `PlaybookRunner` glue.

[h10]: https://noetl.dev/docs/architecture/noetl_global_hybrid_cloud_grid_distributed_architecture_blueprint#h10-architectural-finding-2026-05-30-tree-walker-vs-pull-model
[wiki]: https://github.com/noetl/cli/wiki/executor-crate-architecture

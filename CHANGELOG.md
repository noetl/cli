# Changelog

All notable changes to this project will be documented in this file.

## [4.14.0](https://github.com/noetl/cli/compare/v4.13.0...v4.14.0) (2026-07-13)

### Features

* **provider:** dispatch kind: provider through the local CLI runtime + noetl-tools 3.22.0 ([#64](https://github.com/noetl/cli/issues/64)) ([75edc68](https://github.com/noetl/cli/commit/75edc68cc9bb5ceddb13be4f70b74766cea76f33)), closes [noetl/ai-meta#185](https://github.com/noetl/ai-meta/issues/185) [noetl/worker#183](https://github.com/noetl/worker/issues/183) [noetl/worker#183](https://github.com/noetl/worker/issues/183) [noetl/ai-meta#185](https://github.com/noetl/ai-meta/issues/185) [noetl/ai-meta#189](https://github.com/noetl/ai-meta/issues/189) [noetl/ai-meta#189](https://github.com/noetl/ai-meta/issues/189) [noetl/ai-meta#185](https://github.com/noetl/ai-meta/issues/185) [noetl/tools#87](https://github.com/noetl/tools/issues/87) [noetl/ai-meta#189](https://github.com/noetl/ai-meta/issues/189)

## [4.13.0](https://github.com/noetl/cli/compare/v4.12.0...v4.13.0) (2026-07-11)

### Features

* **ehdb:** `noetl ehdb query tier` raw data-plane tier console ([#178](https://github.com/noetl/cli/issues/178)) ([#62](https://github.com/noetl/cli/issues/62)) ([c168f42](https://github.com/noetl/cli/commit/c168f4266516e0ee4fe9f11df3b13e64b8da208c))

## [4.12.0](https://github.com/noetl/cli/compare/v4.11.0...v4.12.0) (2026-07-07)

### Features

* **ehdb:** noetl ehdb query — read-only CLI console for the EHDB Query Interface ([#61](https://github.com/noetl/cli/issues/61)) ([78c139b](https://github.com/noetl/cli/commit/78c139bad30cb65ed710d4a1641c323112ddb0ab)), closes [noetl/ai-meta#178](https://github.com/noetl/ai-meta/issues/178) [noetl/server#277](https://github.com/noetl/server/issues/277) [noetl/ai-meta#178](https://github.com/noetl/ai-meta/issues/178)

## [4.11.0](https://github.com/noetl/cli/compare/v4.10.0...v4.11.0) (2026-06-12)

### Features

* noetl subscribe — local-mode subscription listener (RFC [#90](https://github.com/noetl/cli/issues/90) Phase 6) ([a005e32](https://github.com/noetl/cli/commit/a005e32213b4ba6d1b0c6674eacc928f91802fe4)), closes [noetl/cli#59](https://github.com/noetl/cli/issues/59)

## [4.10.0](https://github.com/noetl/cli/compare/v4.9.0...v4.10.0) (2026-06-07)

### Features

* **executor:** propagate ToolResult.pending_callback (noetl-tools 2.21 compat) ([8eb0087](https://github.com/noetl/cli/commit/8eb00876e6ab5f41e2ea0346e8e84f32a28f5f1c)), closes [noetl/tools#37](https://github.com/noetl/tools/issues/37) [noetl/cli#55](https://github.com/noetl/cli/issues/55) [noetl/ai-meta#43](https://github.com/noetl/ai-meta/issues/43)

## [4.9.0](https://github.com/noetl/cli/compare/v4.8.0...v4.9.0) (2026-06-04)

### Features

* **events:** extract noetl-events workspace crate (EE-4 PR 1) ([a2d9cc1](https://github.com/noetl/cli/commit/a2d9cc105ba50a56d2486034937f94859a5f4a64)), closes [noetl/ai-meta#49](https://github.com/noetl/ai-meta/issues/49) [noetl/ai-meta#49](https://github.com/noetl/ai-meta/issues/49)

## [4.8.0](https://github.com/noetl/cli/compare/v4.7.0...v4.8.0) (2026-06-02)

### Features

* **executor:** replace gsutil shellout with object_store GCS helper ([d5f26ff](https://github.com/noetl/cli/commit/d5f26ff2b98c90332b426ab9a7ff7c6229c060cb)), closes [noetl/ai-meta#31](https://github.com/noetl/ai-meta/issues/31)

## [4.7.0](https://github.com/noetl/cli/compare/v4.6.0...v4.7.0) (2026-06-01)

### Features

* **arrow-flight-client:** mTLS client identity (R-2.3 Phase C2.4) ([ef1e867](https://github.com/noetl/cli/commit/ef1e867e1e6f53256e918e291a7a8bf66e72e065)), closes [noetl/noetl#648](https://github.com/noetl/noetl/issues/648) [noetl/noetl#648](https://github.com/noetl/noetl/issues/648) [noetl/ai-meta#33](https://github.com/noetl/ai-meta/issues/33)

## [4.6.0](https://github.com/noetl/cli/compare/v4.5.0...v4.6.0) (2026-06-01)

### Features

* **arrow-flight-client:** bearer-token auth on the client (R-2.3 Phase C2.3) ([95fbe97](https://github.com/noetl/cli/commit/95fbe979aa27e7c5c139e05aefb0f42bfa9e9e50)), closes [noetl/noetl#647](https://github.com/noetl/noetl/issues/647) [noetl/noetl#647](https://github.com/noetl/noetl/issues/647) [noetl/ai-meta#33](https://github.com/noetl/ai-meta/issues/33)

## [4.5.0](https://github.com/noetl/cli/compare/v4.4.0...v4.5.0) (2026-06-01)

### Features

* **arrow-flight-client:** TLS client config (R-2.3 Phase C2.2) ([7aa2c36](https://github.com/noetl/cli/commit/7aa2c36d5f0d55a5c7194d6e5981723178fbce7a)), closes [noetl/noetl#646](https://github.com/noetl/noetl/issues/646) [noetl/noetl#646](https://github.com/noetl/noetl/issues/646) [noetl/ai-meta#33](https://github.com/noetl/ai-meta/issues/33)

## [4.4.0](https://github.com/noetl/cli/compare/v4.3.0...v4.4.0) (2026-06-01)

### Features

* **arrow-flight-client:** get_flight_info discovery surface (R-2.3 Phase C1) ([0378e72](https://github.com/noetl/cli/commit/0378e72bc78155564f40b7be3876da975fcfca56)), closes [noetl/noetl#644](https://github.com/noetl/noetl/issues/644) [#41](https://github.com/noetl/cli/issues/41) [noetl/noetl#644](https://github.com/noetl/noetl/issues/644) [noetl/ai-meta#30](https://github.com/noetl/ai-meta/issues/30)

## [4.3.0](https://github.com/noetl/cli/compare/v4.2.0...v4.3.0) (2026-06-01)

### Features

* **arrow-flight-client:** noetl-arrow-flight-client crate (R-2.3 Phase B) ([4bf1891](https://github.com/noetl/cli/commit/4bf1891de32e0be96c9eb33f99a5360728cb2f9e)), closes [noetl/noetl#643](https://github.com/noetl/noetl/issues/643) [#643](https://github.com/noetl/cli/issues/643) [noetl/noetl#643](https://github.com/noetl/noetl/issues/643) [noetl/ai-meta#30](https://github.com/noetl/ai-meta/issues/30)

## [4.2.0](https://github.com/noetl/cli/compare/v4.1.0...v4.2.0) (2026-05-31)

### Features

* **workspace:** add noetl-arrow-cache crate (R-2.1) ([ca5559a](https://github.com/noetl/cli/commit/ca5559a4bf44d3efa0cdd595d64f5b7946d76377)), closes [noetl/ai-meta#30](https://github.com/noetl/ai-meta/issues/30)

## [4.1.0](https://github.com/noetl/cli/compare/v4.0.0...v4.1.0) (2026-05-31)

### Features

* **executor:** enrich ExecutorEvent with optional event_id / worker_id / meta (R-1.2 PR-EE-1, 0.3.1) ([83caadf](https://github.com/noetl/cli/commit/83caadf48a88be1b6d98772198bd545062d731dc)), closes [noetl/ai-meta#30](https://github.com/noetl/ai-meta/issues/30) [noetl/ai-meta#30](https://github.com/noetl/ai-meta/issues/30) [noetl/ai-meta#30](https://github.com/noetl/ai-meta/issues/30)

## [4.0.0](https://github.com/noetl/cli/compare/v3.1.0...v4.0.0) (2026-05-31)

### ⚠ BREAKING CHANGES

* **executor:** to the public trait + struct → 0.2.1 → **0.3.0**
(0.x semver convention: minor bump for breaking).  Bin's
`noetl-executor = { ..., version = "0.3" }` updated to match.

Safe to break: no production consumer imports this module today.
noetl-worker 1.1.2 (currently on crates.io) doesn't use the trait;
PR-2d-2 in the worker repo will be its first adoption against the
new 0.3.0 surface.

## Tests

8 new unit tests including a reusable `MockSource` implementation
that records ack/nack calls in an `Arc<Mutex<Vec<MockAck>>>` for
test assertions:

- `empty_source_returns_none`
- `next_yields_in_order_and_increments_handles`
- `ack_and_nack_recorded_in_order`
- `already_claimed_outcome_carries_handle`
- `retry_later_outcome_carries_error_message`
- `failed_outcome_carries_error_message`
- `command_round_trips_through_serde_with_defaults`
- `command_round_trips_through_serde_with_full_fields`

The `MockSource` itself is testability scaffold that worker tests
in PR-2d-2 can lift verbatim.

Workspace tests: 193 passing (99 noetl-executor unit + 12
integration + 41 noetl + 41 ntl).

## What's next (PR-2d-2 in noetl/worker)

After 0.3.0 publishes:

- Add `NatsCommandSource { subscriber, client, worker_id }` in
  repos/worker/src/nats/source.rs implementing the new trait.
- Translate at the seam between `crate::client::Command` and the
  enriched `noetl_executor::worker::source::Command`.
- Refactor `Worker::process_commands` to drive through
  `CommandSource::next` + `ack`/`nack` instead of the inline
  subscriber + client calls.
- Mock-source unit tests for the dispatcher.

### Features

* **executor:** redesign CommandSource trait with ack lifecycle + richer Command (R-1.2 PR-2d-1, bumps to 0.3.0) ([a673709](https://github.com/noetl/cli/commit/a67370922649b0c1b17e99cb57efd488942bd499)), closes [noetl/ai-meta#30](https://github.com/noetl/ai-meta/issues/30)

## [3.1.0](https://github.com/noetl/cli/compare/v3.0.0...v3.1.0) (2026-05-30)

### Features

* **executor:** add structured Condition + 12-variant Operator (R-1.2 PR-2b, bumps to 0.2.1) ([687f83c](https://github.com/noetl/cli/commit/687f83c153987d66f433b43b9d48fc28057f0a74)), closes [noetl/ai-meta#30](https://github.com/noetl/ai-meta/issues/30)

## [3.0.0](https://github.com/noetl/cli/compare/v2.24.0...v3.0.0) (2026-05-30)

### ⚠ BREAKING CHANGES

* **executor:** to the public types -> bump to 0.2.0 (0.x semver
treats minor bumps as breaking).  Bin's `Cargo.toml` updated to
`noetl-executor = { path = "executor", version = "0.2" }` so the
workspace builds against the local 0.2 source and `cargo publish`
resolves against the published 0.2 line.

## Verification

- `cargo build --workspace` clean.
- `cargo test --workspace`: 174 passing (no test changes needed
  except a single `"exec_test".into()` -> `12345i64` for the
  noop-sink test, which the type change forced).
- No behavior change in any of the existing 80 unit + 12
  integration noetl-executor tests.

## What this unblocks

- PR-2b (noetl/cli): extend `executor::condition` with structured
  Condition + 12-variant Operator; bump to 0.2.1.
- PR-2c (noetl/worker): replace `WorkerEvent` with `ExecutorEvent`
  directly -- no type conversion needed.
- PR-2d (noetl/worker): NATS subscriber implements
  `executor::worker::source::CommandSource` against worker's
  i64-typed snowflake ids -- no conversion needed.

### Miscellaneous Chores

* **executor:** align execution_id to i64 across the crate (R-1.2 PR-2a, bumps to 0.2.0) ([ee95ae7](https://github.com/noetl/cli/commit/ee95ae75437c1f8db3979a1f2fc30748953ac99d)), closes [noetl/ai-meta#30](https://github.com/noetl/ai-meta/issues/30)

## [2.24.0](https://github.com/noetl/cli/compare/v2.23.0...v2.24.0) (2026-05-30)

### Features

* **executor:** extract Tool::Auth + Tool::Sink helpers via bridge (R-1.1 PR-2c-8) ([a58202c](https://github.com/noetl/cli/commit/a58202c2e83639a68c4d3914b856bcb32b9a8ec5)), closes [noetl/ai-meta#30](https://github.com/noetl/ai-meta/issues/30) [noetl/cli#19](https://github.com/noetl/cli/issues/19)

## [2.23.0](https://github.com/noetl/cli/compare/v2.22.0...v2.23.0) (2026-05-30)

### Features

* **executor:** codify § H.10 finding for Tool::Playbook (R-1.1 PR-2c-7) ([7a4c486](https://github.com/noetl/cli/commit/7a4c486615d3036710cf64fb8fad386238ac320f)), closes [noetl/ai-meta#30](https://github.com/noetl/ai-meta/issues/30) [noetl/cli#19](https://github.com/noetl/cli/issues/19)

## [2.22.0](https://github.com/noetl/cli/compare/v2.21.0...v2.22.0) (2026-05-30)

### Features

* **executor:** wire Tool::DuckDb through noetl-tools bridge (R-1.1 PR-2c-6) ([b5d1111](https://github.com/noetl/cli/commit/b5d1111647650aa73703787833ea050fa781b562)), closes [noetl/ai-meta#30](https://github.com/noetl/ai-meta/issues/30) [noetl/cli#19](https://github.com/noetl/cli/issues/19)

## [2.21.0](https://github.com/noetl/cli/compare/v2.20.0...v2.21.0) (2026-05-30)

### Features

* **executor:** wire Tool::Http through noetl-tools bridge (R-1.1 PR-2c-5) ([d504f71](https://github.com/noetl/cli/commit/d504f7190b65138e3955b41d7462edffe1b6fbbe)), closes [noetl/ai-meta#36](https://github.com/noetl/ai-meta/issues/36) [noetl/cli#19](https://github.com/noetl/cli/issues/19)

## [2.20.0](https://github.com/noetl/cli/compare/v2.19.0...v2.20.0) (2026-05-30)

### Features

* **executor:** wire Tool::Shell through noetl-tools bridge (R-1.1 PR-2c-4) ([9c747e1](https://github.com/noetl/cli/commit/9c747e1358616126d2ac894dd630810216c907f4))

## [2.19.0](https://github.com/noetl/cli/compare/v2.18.0...v2.19.0) (2026-05-30)

### Features

* **executor:** wire Tool::Rhai through noetl-tools bridge (R-1.1 PR-2c-3) ([fe07488](https://github.com/noetl/cli/commit/fe074880c29330baf43ac007b1a1b02626006500))

## [2.18.0](https://github.com/noetl/cli/compare/v2.17.1...v2.18.0) (2026-05-30)

### Features

* **executor:** flesh out tools_bridge adapters (R-1.1 PR-2c-2) ([7750cf2](https://github.com/noetl/cli/commit/7750cf23ef4cc084e237ae5093c2683869569127))

## [2.17.1](https://github.com/noetl/cli/compare/v2.17.0...v2.17.1) (2026-05-29)

### Bug Fixes

* **auth:** include region segment in Auth0 dashboard URL for regional tenants ([7330815](https://github.com/noetl/cli/commit/733081583371622817917dc4397b85a44b33f2bc)), closes [noetl/ai-meta#18](https://github.com/noetl/ai-meta/issues/18) [noetl/ai-meta#18](https://github.com/noetl/ai-meta/issues/18)

## [2.17.0](https://github.com/noetl/cli/compare/v2.16.0...v2.17.0) (2026-05-28)

### Features

* **cli:** port-forward port-conflict probe + global --context flag ([afe03e9](https://github.com/noetl/cli/commit/afe03e9d86e38743235a5a38a8e035a1ea8675c9))

## [2.16.0](https://github.com/noetl/cli/compare/v2.15.0...v2.16.0) (2026-05-28)

### Features

* **cli:** noetl context init --from-gateway ([16b46e5](https://github.com/noetl/cli/commit/16b46e576dd80bd8a6b754b1578cae32dc4fc9b8)), closes [#16](https://github.com/noetl/cli/issues/16) [#13](https://github.com/noetl/cli/issues/13) [#14](https://github.com/noetl/cli/issues/14) [#16](https://github.com/noetl/cli/issues/16) [#124](https://github.com/noetl/cli/issues/124)

## [2.15.0](https://github.com/noetl/cli/compare/v2.14.3...v2.15.0) (2026-05-28)

### Features

* **cli:** context update + gateway 401 + PKCE callback URL hints ([e64b127](https://github.com/noetl/cli/commit/e64b12721048695241790fb2e353dfcfe5d786a5))
* **cli:** noetl context port-forward — managed kubectl tunnel ([1e7f69a](https://github.com/noetl/cli/commit/1e7f69ac3583e66fa7930f5a4544b79349ccf4ac)), closes [#13](https://github.com/noetl/cli/issues/13) [#13](https://github.com/noetl/cli/issues/13) [#13](https://github.com/noetl/cli/issues/13)

## [2.14.3](https://github.com/noetl/cli/compare/v2.14.2...v2.14.3) (2026-05-18)

### Bug Fixes

* **exec:** normalize distributed workload overrides ([22065cc](https://github.com/noetl/cli/commit/22065cc251733798d3f0ab303c6df4f6b09c76c1))

## [2.14.2](https://github.com/noetl/cli/compare/v2.14.1...v2.14.2) (2026-05-15)

### Bug Fixes

* publish cli release assets after semantic release ([d708e74](https://github.com/noetl/cli/commit/d708e747e08f0654445343b3a032a3175f06ed3d))
* skip optional package publishers without usable tokens ([8897369](https://github.com/noetl/cli/commit/889736920c2cba821efc75cef6eb30960d07625c))

## [2.14.1](https://github.com/noetl/cli/compare/v2.14.0...v2.14.1) (2026-05-05)

### Bug Fixes

* **cli:** capture kind:shell stdout into step_results ([86d9a93](https://github.com/noetl/cli/commit/86d9a939486f9edfab36f75c840f83e76fa8db5b)), closes [cli#8](https://github.com/noetl/cli/issues/8)

## [2.14.0](https://github.com/noetl/cli/compare/v2.13.0...v2.14.0) (2026-05-05)

### Features

* **cli:** --json on local runtime emits RunOutcome envelope ([f528781](https://github.com/noetl/cli/commit/f528781ab12ddc76ad0f246583548af05db25474))

## [2.13.0](https://github.com/noetl/cli/compare/v2.12.1...v2.13.0) (2026-03-30)

### Features

* **dsl:** add DSL v2 input field support for playbook composition ([fd4c3ee](https://github.com/noetl/cli/commit/fd4c3ee2d2f07270b97df97f50ae1c0c9aacd146))

## [2.12.1](https://github.com/noetl/cli/compare/v2.12.0...v2.12.1) (2026-03-28)

### Bug Fixes

* **docker:** build and package noetl binary with clean context ([92ab3e7](https://github.com/noetl/cli/commit/92ab3e7cba6b05c1f76ad97677ccbeecd25bbe07))
* **publish:** bump 2.12.1 and fix ai args borrow ([6fbf440](https://github.com/noetl/cli/commit/6fbf4409038ea7d832f0c0499dad8927fa1e6c5e))

## [2.12.0](https://github.com/noetl/cli/compare/v2.11.0...v2.12.0) (2026-03-27)

### Features

* **cli:** add codex passthrough, doctor check, and ai scaffold command ([1bf127e](https://github.com/noetl/cli/commit/1bf127eb60b71658d2bc9c9a947209213808e048))

## [2.11.0](https://github.com/noetl/cli/compare/v2.10.0...v2.11.0) (2026-03-20)

### Features

* add localhost PKCE browser login flow ([b49e9cf](https://github.com/noetl/cli/commit/b49e9cfc00cc8f71455fc74e7f025be0f56f9cf3))

## [2.10.0](https://github.com/noetl/cli/compare/v2.9.0...v2.10.0) (2026-03-20)

### Features

* add browser device auth flow and optional auth0 secret ([3ef88b3](https://github.com/noetl/cli/commit/3ef88b39692ab271dc639b7ba7c1d14ec8b8ecfc))

## [2.9.0](https://github.com/noetl/cli/compare/v2.8.8...v2.9.0) (2026-03-19)

### Features

* add console REPL and preserve context auth state on updates ([6eb8840](https://github.com/noetl/cli/commit/6eb88407af4975e992d515cc18fd3de1b2d928d4))

## [2.8.8](https://github.com/noetl/cli/compare/v2.8.7...v2.8.8) (2026-03-17)

### Bug Fixes

* add execute rerun command and canonical payload contract ([a2f4ca5](https://github.com/noetl/cli/commit/a2f4ca5e9114145871fb52ed7ce463a676409fb6))

## 2.8.7 (2026-03-02)

### Bug Fixes

* make release input parsing event-safe ([6cca33f](https://github.com/noetl/cli/commit/6cca33f938cd49006f492e67cbd6d145b2acd9e8))
* release workflows on push and semantic auth ([593ca30](https://github.com/noetl/cli/commit/593ca30a2085e735c373f7ca5fc6bc25a45d28d5))
* remove secret expressions from workflow conditions ([f5317de](https://github.com/noetl/cli/commit/f5317de29b936ddcd56b90cf4f087f8f5698165e))

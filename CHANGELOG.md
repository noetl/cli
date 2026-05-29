# Changelog

All notable changes to this project will be documented in this file.

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

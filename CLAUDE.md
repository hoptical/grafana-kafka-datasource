# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A Grafana data source plugin that streams Kafka topics into Grafana dashboards in real time. It has two halves that must be kept in sync:

- **Backend** (Go, `pkg/`): a Grafana backend plugin (`grafana-plugin-sdk-go`) that connects to Kafka via `segmentio/kafka-go`, decodes messages, and streams data frames to the frontend over Grafana Live.
- **Frontend** (TypeScript/React, `src/`): the Config Editor and Query Editor UI, plus a thin `DataSource` class that mostly delegates to the backend (`DataSourceWithBackend`).

## Common Commands

### Frontend

- `pnpm install` — install deps (pnpm is pinned via `packageManager`; use `corepack enable && corepack install`)
- `pnpm run dev` — webpack watch build
- `pnpm run build` — production build
- `pnpm run typecheck` — `tsc --noEmit`
- `pnpm run lint` / `pnpm run lint:fix` — ESLint (+ Prettier on fix)
- `pnpm run lint:md` — markdownlint for `**/*.md`
- `pnpm run test` — Jest watch mode, only changed files
- `pnpm run test:ci` — Jest single run (used in CI)
- `pnpm exec jest src/QueryEditor.test.tsx` — run a single frontend test file
- `pnpm run e2e` — Playwright E2E tests (needs Grafana/Kafka running, see below)

### Backend (Go)

- `mage buildAll` — cross-compile the backend binary for all platforms (also `mage build:backend` for current platform, wired to `pnpm run build:backend`)
- `mage test` — run Go unit tests
- `mage testRace` — run Go tests with `-race` (required in CI)
- `mage coverage` — run tests with coverage (used in CI)
- `golangci-lint run` — backend lint (CI uses `golangci-lint-action`)
- `go test ./pkg/plugin/... -run TestName -v` — run a single Go test
- `go test ./pkg/plugin/... -bench BenchmarkName -benchmem` — run a specific benchmark (many exist, e.g. `frame_builder_bench_test.go`, `json_utils_bench_test.go`, `stream_manager_bench_test.go`)

### Local dev environment

- `pnpm run server` — `docker compose up --build`: starts Grafana + Kafka (KRaft mode, SASL/SSL listeners) + Schema Registry + Redpanda Console, with this plugin mounted. Must be run **on the host**, not inside the Dev Container (no Docker daemon access there).
- Inside the Dev Container: run `pnpm`/`go`/`mage` commands; if you need Grafana/Kafka, point at the host with `GRAFANA_URL=http://host.docker.internal:3000` (macOS/Windows Docker Desktop ignores `--network=host`).
- Sample data producers live in `example/go` and `example/python` (see `example/README.md`) for generating JSON/Avro/Protobuf/plaintext test messages.

### Pre-commit

`pre-commit install` wires up ESLint+Prettier, `golangci-lint run --fix`, and `markdownlint-cli2 --fix` on commit.

## Architecture

### Backend request flow (`pkg/plugin/plugin.go`)

`KafkaDatasource` implements the SDK's `QueryDataHandler`, `CheckHealthHandler`, `StreamHandler`, and `CallResourceHandler`. Key entry points:

- `QueryData` / `query` — non-streaming query path (largely a no-op placeholder; real data arrives via streaming).
- `CallResource` — routes `/partitions`, `/topics`, `/schema-registry/validate`, `/avro/validate`, `/protobuf/validate` resource calls (topic/partition discovery and schema validation used by the Query Editor UI).
- `CheckHealth` — datasource connectivity check (used by the Config Editor "Save & Test").
- `SubscribeStream` / `RunStream` — the real work. `RunStream` spawns one goroutine per partition (`StreamManager.readFromPartition`) that pulls messages via `KafkaClientAPI.ConsumerPull`, converts them to Grafana `data.Frame`s, and fans them in over a buffered channel (`streamMessageBuffer = 100`) to a single sender loop that pushes frames to the `backend.StreamSender`.

`KafkaClientAPI` is the interface `kafka_client.KafkaClient` implements; it's abstracted so `pkg/plugin` tests can inject fakes without a real Kafka broker.

### Message processing pipeline (`pkg/plugin/stream_manager.go`)

`StreamManager.ProcessMessageToFrame` is the core per-message pipeline: decode (JSON/Avro/Protobuf/Plaintext) → flatten → build a `data.Frame` field-by-field via `FieldBuilder` (`frame_builder.go`). Line Protocol messages take a separate path (`ProcessMessageFrames` in `lineprotocol_frame.go`) because one Kafka message can expand into multiple LP lines → multiple frame rows with a fixed long-format schema (`Time | _measurement | _field | value | value_str | <tag columns> | offset`).

Field ordering matters for Grafana Live's schema-per-channel model: `time`, `partition` (if "all"), `offset`, key fields (alphabetical), then value fields (alphabetical) — see `docs/MESSAGE_KEYS.md`. `sortedFlatKeys` caches the sorted key order across messages when the field set is unchanged (see Performance section).

Avro/Protobuf schema resolution supports both inline schemas and Schema Registry lookups (Confluent wire format: magic byte + 4-byte schema ID, plus a protobuf message-index array). Registry schemas are cached by ID/subject (`getSchemaWithCache` family) — see `docs/PERFORMANCE_OPTIMIZATIONS.md`.

`createErrorFrame` / `createLineProtocolErrorFrame` produce a schema-compatible error frame (same columns, an added error tag) so a bad message doesn't break the live channel's schema for subsequent good messages.

`frame_batcher.go` (`frameMicroBatcher`) coalesces multiple same-schema frames produced in a short window into fewer, multi-row frames before sending, to cut per-message stream overhead.

### Performance feature flags (`pkg/perfflags/perfflags.go`)

Production always uses the optimized path. Benchmark-only reproduction of the old behavior now happens through explicit constructor options and dedicated benchmark variants, not shipped env vars. `pkg/kafka_client/message_decoder.go` owns the Avro/Protobuf cache behavior, `StreamManager` options control field-order caching, and `BenchmarkWorkflow_NoOptimizations` reproduces the combined old path for whole-workflow measurement. When touching these hot paths, preserve both the optimized production path and the explicit benchmark-only old-path coverage in the corresponding `*_bench_test.go` files. See `docs/PERFORMANCE_OPTIMIZATIONS.md` for the methodology (Go benchmarks + `pprof` + `benchstat`) and the benchmark-name normalization step needed for `benchstat` comparisons.

### Frontend (`src/`)

- `module.ts` registers `DataSource` + `ConfigEditor` + `QueryEditor` as the `DataSourcePlugin`.
- `datasource.ts` (`DataSource extends DataSourceWithBackend`) does query-time work the backend can't: template variable interpolation (`applyTemplateVariables`), query filtering (`filterQuery`), and building the Grafana Live channel path/session id (`PAGE_LOAD_SESSION` forces a fresh channel per page load so a hard refresh always restarts the stream at the correct offset instead of reattaching to a draining reader).
- `types.ts` is the single source of truth for query/datasource-options shapes (`KafkaQuery`, `KafkaDataSourceOptions`, `KeyFormat`, `MessageFormat`, etc.) and their defaults (`defaultQuery`, `defaultDataSourceOptions`) — mirror any new backend query field here.
- `QueryEditor.tsx` / `ConfigEditor.tsx` are the two Grafana-rendered editor UIs; most fields there map 1:1 to a `KafkaQuery`/`KafkaDataSourceOptions` field.

### Cross-cutting: adding a new query-level feature

Changes that add a new query option (like `keyFormat`) typically touch all of: `src/types.ts` (interface + enum + default), `src/QueryEditor.tsx` (UI control), `pkg/plugin/plugin.go` (`queryModel` JSON decoding), `pkg/plugin/stream_manager.go` (`StreamConfig` + processing logic), and usually a corresponding doc in `docs/`. `docs/MESSAGE_KEYS.md` is a good reference for the expected shape of such a change (data model, UI, docs, tests) end-to-end.

## Testing conventions

- Go unit tests live alongside the code they test (`*_test.go`); benchmarks are separate `*_bench_test.go` files. Tests set `log.DefaultLogger = log.NewNullLogger()` in `TestMain` to avoid debug-log overhead skewing benchmark timings.
- Frontend unit tests live in `src/__tests__/*.test.tsx` (Jest + Testing Library).
- Playwright E2E specs (`tests/*.spec.ts`) are split by message format (`queryEditor-json`, `-avro`, `-protobuf`, `-plaintext`, `-lineprotocol`, `-keys`) plus `configEditor.spec.ts`; they run against the Docker Compose stack (real Grafana + Kafka + Schema Registry).

## Notes

- Go module: `github.com/hoptical/grafana-kafka-datasource`, Go 1.26.5, uses `linkedin/goavro`, `bufbuild/protocompile`, `segmentio/kafka-go`.
- Tool versions are pinned in `mise.toml` (go, node, mage, pnpm, golangci-lint) and `package.json#packageManager` (pnpm).
- CI (`.github/workflows/ci.yml`) runs, in order: frontend typecheck/lint/lint:md/test/build, then backend lint/coverage/testRace/buildAll, then builds the example Go producer, then Playwright E2E against multiple Grafana versions.

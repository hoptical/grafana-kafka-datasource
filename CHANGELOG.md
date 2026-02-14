# Changelog

## [v1.4.0](https://github.com/hoptical/grafana-kafka-datasource/tree/v1.4.0)

[Full Changelog](https://github.com/hoptical/grafana-kafka-datasource/compare/v1.3.0...v1.4.0)

- Feat: add **Protobuf** Support ([#128](https://github.com/hoptical/grafana-kafka-datasource/pull/128))
- Chore: bump @grafana/create-plugin configuration to 6.8.4 ([#129](https://github.com/hoptical/grafana-kafka-datasource/pull/129))
- Feat: improve backend tests to increase coverage and reliability ([#130](https://github.com/hoptical/grafana-kafka-datasource/pull/130))
- Feat: update backend and frontend packages to fix security vulnerabilities ([#130](https://github.com/hoptical/grafana-kafka-datasource/pull/130))

## [v1.3.0](https://github.com/hoptical/grafana-kafka-datasource/tree/v1.3.0)

[Full Changelog](https://github.com/hoptical/grafana-kafka-datasource/compare/v1.2.3...v1.3.0)

- Enhancement: Upgrade dependencies to address security vulnerabilities ([#126](https://github.com/hoptical/grafana-kafka-datasource/pull/126))
- Fix: Improve SASL defaulting, error clarity, and health check timeout handling ([#123](https://github.com/hoptical/grafana-kafka-datasource/pull/123))
- Feat: Add support for `refid` and `alias` in queries ([#122](https://github.com/hoptical/grafana-kafka-datasource/pull/122))
- Fix: Multiple query support by making streaming stateless ([#121](https://github.com/hoptical/grafana-kafka-datasource/pull/121))

## [v1.2.3](https://github.com/hoptical/grafana-kafka-datasource/tree/v1.2.3)

[Full Changelog](https://github.com/hoptical/grafana-kafka-datasource/compare/v1.2.2...v1.2.3)

- Enhancement: Change info levels to debug for all requests in the backend ([#118](https://github.com/hoptical/grafana-kafka-datasource/pull/118))

## [v1.2.2](https://github.com/hoptical/grafana-kafka-datasource/tree/v1.2.2)

[Full Changelog](https://github.com/hoptical/grafana-kafka-datasource/compare/v1.2.1...v1.2.2)

- Feat: Add PDC support for schema registry using grafana-plugin-sdk-http-client ([#113](https://github.com/hoptical/grafana-kafka-datasource/pull/113))
- Enhancement: Add linting for frontend and backend with pre-commit hooks and CI updates ([#115](https://github.com/hoptical/grafana-kafka-datasource/pull/115))
- Enhancement: Replace inline styling with emotion styling and update related tests ([#116](https://github.com/hoptical/grafana-kafka-datasource/pull/116))

## [v1.2.1](https://github.com/hoptical/grafana-kafka-datasource/tree/v1.2.1)

[Full Changelog](https://github.com/hoptical/grafana-kafka-datasource/compare/v1.2.0...v1.2.1)

- Refactor: Improve e2e test stability and maintainability ([#112](https://github.com/hoptical/grafana-kafka-datasource/pull/112))

## [v1.2.0](https://github.com/hoptical/grafana-kafka-datasource/tree/v1.2.0)

[Full Changelog](https://github.com/hoptical/grafana-kafka-datasource/compare/v1.1.0...v1.2.0)

- Feat: Implement Avro support ([#96](https://github.com/hoptical/grafana-kafka-datasource/pull/96)) — add Avro parsing and support for Avro-encoded messages in the plugin.
- Chore: Bump `@grafana/create-plugin` configuration to 6.4.3 ([#109](https://github.com/hoptical/grafana-kafka-datasource/pull/109))

## [v1.1.0](https://github.com/hoptical/grafana-kafka-datasource/tree/v1.1.0)

[Full Changelog](https://github.com/hoptical/grafana-kafka-datasource/compare/v1.0.1...v1.1.0)

- Fix: Address security issues in dependencies and configuration ([#107](https://github.com/hoptical/grafana-kafka-datasource/pull/107))
- Fix: Use type registry with nullable types to prevent data loss when encountering null values ([#104](https://github.com/hoptical/grafana-kafka-datasource/pull/104))
- Perf: Pre-allocate frame fields to eliminate append overhead ([#106](https://github.com/hoptical/grafana-kafka-datasource/pull/106))
- Feat: Configurable JSON flatten limits ([#99](https://github.com/hoptical/grafana-kafka-datasource/pull/99))
- Add: Provisioned dashboard ([#94](https://github.com/hoptical/grafana-kafka-datasource/pull/94))
- Fix: Mutation issue in config editor and query editor ([#91](https://github.com/hoptical/grafana-kafka-datasource/pull/91))
- Fix: e2e tests for query editor for Grafana versions later than 12.2 ([#102](https://github.com/hoptical/grafana-kafka-datasource/pull/102))
- Add: Devcontainer for development ([#90](https://github.com/hoptical/grafana-kafka-datasource/pull/90))
- Test: Improve mutation tests with deep freezing ([#93](https://github.com/hoptical/grafana-kafka-datasource/pull/93))
- CI: Skip publish-report for fork PRs ([#103](https://github.com/hoptical/grafana-kafka-datasource/pull/103))

## [v1.0.1](https://github.com/hoptical/grafana-kafka-datasource/tree/v1.0.1)

[Full Changelog](https://github.com/hoptical/grafana-kafka-datasource/compare/v1.0.0...v1.0.1)

- Restructure and split documentation (README, docs/development.md, docs/contributing.md, docs/code_of_conduct.md)
- Add “Create Plugin Update” GitHub Action
- Add release workflow pre‐check to ensure tag exists in CHANGELOG
- Bump plugin and package versions to 1.0.1

## [v1.0.0](https://github.com/hoptical/grafana-kafka-datasource/tree/v1.0.0) (2025-08-14)

[Full Changelog](https://github.com/hoptical/grafana-kafka-datasource/compare/v0.6.0...v1.0.0)

- Add support for the nested JSON [\#83](https://github.com/hoptical/grafana-kafka-datasource/pull/83) ([hoptical](https://github.com/hoptical))
- Support for Nested JSON via Automatic Flattening [\#79](https://github.com/hoptical/grafana-kafka-datasource/issues/79)

## [v0.6.0](https://github.com/hoptical/grafana-kafka-datasource/tree/v0.6.0) (2025-08-13)

[Full Changelog](https://github.com/hoptical/grafana-kafka-datasource/compare/v0.5.0...v0.6.0)

- Use kafka message time as default option [\#82](https://github.com/hoptical/grafana-kafka-datasource/pull/82) ([hoptical](https://github.com/hoptical))
- Add options to auto offset reset [\#81](https://github.com/hoptical/grafana-kafka-datasource/pull/81) ([hoptical](https://github.com/hoptical))
- Add/support all partitions [\#80](https://github.com/hoptical/grafana-kafka-datasource/pull/80) ([hoptical](https://github.com/hoptical))

- Topic Selection with Autocomplete [\#78](https://github.com/hoptical/grafana-kafka-datasource/issues/78)
- Improve the offset reset field [\#76](https://github.com/hoptical/grafana-kafka-datasource/issues/76)
- Add support for selecting all partitions [\#75](https://github.com/hoptical/grafana-kafka-datasource/issues/75)
- Is it possible to reset offset? [\#47](https://github.com/hoptical/grafana-kafka-datasource/issues/47)
- Clarify the options of the timestamp mode [\#77](https://github.com/hoptical/grafana-kafka-datasource/issues/77)

## [v0.5.0](https://github.com/hoptical/grafana-kafka-datasource/tree/v0.5.0) (2025-08-01)

[Full Changelog](https://github.com/hoptical/grafana-kafka-datasource/compare/v0.4.0...v0.5.0)

- Instruction for provisioning [\#43](https://github.com/hoptical/grafana-kafka-datasource/issues/43)
- Add tests [\#27](https://github.com/hoptical/grafana-kafka-datasource/issues/27)
- Add/tls config [\#73](https://github.com/hoptical/grafana-kafka-datasource/pull/73) ([hoptical](https://github.com/hoptical))
- add backend tests [\#72](https://github.com/hoptical/grafana-kafka-datasource/pull/72) ([hoptical](https://github.com/hoptical))
- Add/test [\#71](https://github.com/hoptical/grafana-kafka-datasource/pull/71) ([hoptical](https://github.com/hoptical))
- Update/yarn to npm [\#70](https://github.com/hoptical/grafana-kafka-datasource/pull/70) ([hoptical](https://github.com/hoptical))

- Replace deprecated Grafana SCSS styles [\#48](https://github.com/hoptical/grafana-kafka-datasource/issues/48)
- roll back required permissions for the attestations [\#74](https://github.com/hoptical/grafana-kafka-datasource/pull/74) ([hoptical](https://github.com/hoptical))

- Source Dropdown in query editor [\#24](https://github.com/hoptical/grafana-kafka-datasource/issues/24)

## [v0.4.0](https://github.com/hoptical/grafana-kafka-datasource/tree/v0.4.0) (2025-07-19)

[Full Changelog](https://github.com/hoptical/grafana-kafka-datasource/compare/v0.3.0...v0.4.0)

- Fix/submission warnings [\#69](https://github.com/hoptical/grafana-kafka-datasource/pull/69) ([hoptical](https://github.com/hoptical))

- Create FUNDING.yml [\#68](https://github.com/hoptical/grafana-kafka-datasource/pull/68) ([hoptical](https://github.com/hoptical))

## [v0.3.0](https://github.com/hoptical/grafana-kafka-datasource/tree/v0.3.0) (2025-05-24)

[Full Changelog](https://github.com/hoptical/grafana-kafka-datasource/compare/v0.2.0...v0.3.0)

- Fix/release [\#65](https://github.com/hoptical/grafana-kafka-datasource/pull/65) ([hoptical](https://github.com/hoptical))
- Fix/streaming logic [\#64](https://github.com/hoptical/grafana-kafka-datasource/pull/64) ([hoptical](https://github.com/hoptical))
- add value-offset to go producer example [\#63](https://github.com/hoptical/grafana-kafka-datasource/pull/63) ([hoptical](https://github.com/hoptical))
- Update/grafana go sdk [\#62](https://github.com/hoptical/grafana-kafka-datasource/pull/62) ([hoptical](https://github.com/hoptical))
- Add/golang example [\#60](https://github.com/hoptical/grafana-kafka-datasource/pull/60) ([hoptical](https://github.com/hoptical))
- Add/redpanda console [\#58](https://github.com/hoptical/grafana-kafka-datasource/pull/58) ([hoptical](https://github.com/hoptical))
- Add Kafka to docker compose [\#54](https://github.com/hoptical/grafana-kafka-datasource/pull/54) ([sizovilya](https://github.com/sizovilya))

- \[BUG\] Cannot connect to the brokers [\#55](https://github.com/hoptical/grafana-kafka-datasource/issues/55)
- \[BUG\] Plugin Unavailable [\#45](https://github.com/hoptical/grafana-kafka-datasource/issues/45)
- plugin unavailable [\#44](https://github.com/hoptical/grafana-kafka-datasource/issues/44)
- \[BUG\] 2 Kafka panels on 1 dashboard [\#39](https://github.com/hoptical/grafana-kafka-datasource/issues/39)
- \[BUG\] The yarn.lock is out of sync with package.json since the 0.2.0 commit [\#35](https://github.com/hoptical/grafana-kafka-datasource/issues/35)
- User has to refresh page to trigger streaming [\#28](https://github.com/hoptical/grafana-kafka-datasource/issues/28)
- Mage Error [\#6](https://github.com/hoptical/grafana-kafka-datasource/issues/6)
- use access policy token instead of the legacy one [\#66](https://github.com/hoptical/grafana-kafka-datasource/pull/66) ([hoptical](https://github.com/hoptical))
- Fix switch component [\#61](https://github.com/hoptical/grafana-kafka-datasource/pull/61) ([sizovilya](https://github.com/sizovilya))
- Change Kafka driver [\#57](https://github.com/hoptical/grafana-kafka-datasource/pull/57) ([sizovilya](https://github.com/sizovilya))
- Provide default options to UI [\#56](https://github.com/hoptical/grafana-kafka-datasource/pull/56) ([sizovilya](https://github.com/sizovilya))
- Fix CI Failure [\#53](https://github.com/hoptical/grafana-kafka-datasource/pull/53) ([hoptical](https://github.com/hoptical))

- Add developer-friendly environment [\#52](https://github.com/hoptical/grafana-kafka-datasource/issues/52)
- Migrate from Grafana toolkit [\#50](https://github.com/hoptical/grafana-kafka-datasource/issues/50)
- Can you make an arm64-compatible version of this plugin? [\#49](https://github.com/hoptical/grafana-kafka-datasource/issues/49)
- Add Authentication & Authorization Configuration [\#20](https://github.com/hoptical/grafana-kafka-datasource/issues/20)

- Migrate from toolkit [\#51](https://github.com/hoptical/grafana-kafka-datasource/pull/51) ([sizovilya](https://github.com/sizovilya))
- Update README.md [\#46](https://github.com/hoptical/grafana-kafka-datasource/pull/46) ([0BVer](https://github.com/0BVer))
- Add support for using AWS MSK Managed Kafka [\#38](https://github.com/hoptical/grafana-kafka-datasource/pull/38) ([ksquaredkey](https://github.com/ksquaredkey))
- Issue 35 update readme node14 for dev \(\#1\) [\#37](https://github.com/hoptical/grafana-kafka-datasource/pull/37) ([ksquaredkey](https://github.com/ksquaredkey))

## [v0.2.0](https://github.com/hoptical/grafana-kafka-datasource/tree/v0.2.0) (2022-07-26)

[Full Changelog](https://github.com/hoptical/grafana-kafka-datasource/compare/v0.1.0...v0.2.0)

- PLUGIN NOT AVAILABLE [\#31](https://github.com/hoptical/grafana-kafka-datasource/issues/31)
- Issue6 [\#25](https://github.com/hoptical/grafana-kafka-datasource/pull/25) ([hoptical](https://github.com/hoptical))

- Customizable Timestamp [\#13](https://github.com/hoptical/grafana-kafka-datasource/issues/13)

- V0.2.0 [\#34](https://github.com/hoptical/grafana-kafka-datasource/pull/34) ([hoptical](https://github.com/hoptical))
- V0.2.0 [\#33](https://github.com/hoptical/grafana-kafka-datasource/pull/33) ([hoptical](https://github.com/hoptical))
- V0.2.0 [\#32](https://github.com/hoptical/grafana-kafka-datasource/pull/32) ([hoptical](https://github.com/hoptical))
- Issue13 [\#26](https://github.com/hoptical/grafana-kafka-datasource/pull/26) ([hoptical](https://github.com/hoptical))

## [v0.1.0](https://github.com/hoptical/grafana-kafka-datasource/tree/v0.1.0) (2021-11-14)

[Full Changelog](https://github.com/hoptical/grafana-kafka-datasource/compare/cff631297154c3d90f8f26d56b3f9ca77c3e3369...v0.1.0)

- Use channel instead of time.after [\#14](https://github.com/hoptical/grafana-kafka-datasource/issues/14)
- Code base cleaning [\#12](https://github.com/hoptical/grafana-kafka-datasource/issues/12)
- Handle Messages with the specified format [\#5](https://github.com/hoptical/grafana-kafka-datasource/issues/5)
- Specify query editor [\#4](https://github.com/hoptical/grafana-kafka-datasource/issues/4)
- Specify Kafka Configuration in Config Editor [\#3](https://github.com/hoptical/grafana-kafka-datasource/issues/3)
- consumer-group stuck in rebalancing situation [\#2](https://github.com/hoptical/grafana-kafka-datasource/issues/2)
- Release minors [\#19](https://github.com/hoptical/grafana-kafka-datasource/pull/19) ([hoptical](https://github.com/hoptical))
- modify description and minor information in plugin [\#18](https://github.com/hoptical/grafana-kafka-datasource/pull/18) ([hoptical](https://github.com/hoptical))
- Issue3 [\#9](https://github.com/hoptical/grafana-kafka-datasource/pull/9) ([hoptical](https://github.com/hoptical))
- Issue5 [\#8](https://github.com/hoptical/grafana-kafka-datasource/pull/8) ([hoptical](https://github.com/hoptical))

- Readme enhancement [\#17](https://github.com/hoptical/grafana-kafka-datasource/pull/17) ([hoptical](https://github.com/hoptical))
- Issue12 [\#16](https://github.com/hoptical/grafana-kafka-datasource/pull/16) ([smortexa](https://github.com/smortexa))
- messages are handled via channel instead of time.after [\#15](https://github.com/hoptical/grafana-kafka-datasource/pull/15) ([hoptical](https://github.com/hoptical))
- Query editor [\#11](https://github.com/hoptical/grafana-kafka-datasource/pull/11) ([hoptical](https://github.com/hoptical))
- Issue2 [\#7](https://github.com/hoptical/grafana-kafka-datasource/pull/7) ([hoptical](https://github.com/hoptical))
- Kafka client [\#1](https://github.com/hoptical/grafana-kafka-datasource/pull/1) ([hoptical](https://github.com/hoptical))

# Contributing to Kafka Datasource for Grafana

Thank you for considering contributing! We welcome issues, feature requests, and pull requests.

## How to Contribute

1. Fork the repository and create a branch from `main`.
2. Open an issue to discuss larger changes before starting work.
3. Submit a pull request with a clear description and tests where appropriate.

## Developer Setup

### Dev Container (Recommended)

This repository includes a configured Dev Container for a consistent development environment (Node.js, Go, Mage, Playwright, etc.). The devcontainer provides a pre-configured workspace for most development work, but a few host-specific notes are important below.

How to use:

1. Open the project in VS Code with the Remote - Containers / Dev Containers extension.
2. Choose "Reopen in Container" when prompted; the container will build and install tools.
3. Use the integrated terminal to run `npm`, `go`, and `mage` commands inside the container.

Important notes:

- Docker access: the Dev Container typically does not include Docker daemon access. If you need to run Docker Compose (for example to start Grafana/Kafka during E2E), run `npm run server` on your host machine rather than inside the container.
- Networking: On Linux the container can often use `--network=host` to allow services to bind to the host network. On macOS and Windows Docker Desktop ignores `--network=host`; when running E2E tests from inside the container set `GRAFANA_URL=http://host.docker.internal:3000` so the container can reach Grafana running on the host.
- Playwright: Playwright browsers are pre-installed for the non-root user in the Dev Container so E2E tests should work out-of-the-box; if you run tests locally you may need to run `npx playwright install` or `npm exec playwright install chromium --with-deps` depending on your environment.
- Rebuild: If you change devcontainer toolchain or dependencies in the `Dockerfile` or devcontainer configuration, rebuild the container from VS Code: `Dev Containers: Rebuild Container`.

Recommended workflow:

1. On host: `npm run server` (if you need Grafana/Kafka via Docker Compose).
2. Inside container: run development, build, tests, and linting commands (`npm run dev`, `npm run build`, `npm run test`, `mage`, etc.).

### Prerequisites

- Node.js >= 22
- Go >= 1.25.6
- Mage >= 1.15.0 (for backend builds)
- Docker (for running Grafana/Kafka locally)
- Python 3 (for `pre-commit` hooks)

### Local Setup

1. Clone the repository:

```bash
git clone https://github.com/hoptical/grafana-kafka-datasource.git
cd grafana-kafka-datasource
```

2. Install frontend dependencies:

```bash
npm install
```

3. Install pre-commit and enable hooks:

```bash
pip install pre-commit
pre-commit install
```

## Frontend (React)

- Install dependencies: `npm install`
- Run in development mode: `npm run dev`
- Build for production: `npm run build`
- Unit tests: `npm run test` or `npm run test:ci`
- E2E tests (Playwright):
  - On host (starts Grafana/Kafka): `npm run server`
  - Inside container or locally: `npm run e2e`

## Backend (Golang)

- Update plugin SDK and tidy modules:

```bash
go get -u github.com/grafana/grafana-plugin-sdk-go
go mod tidy
```

- Build backend plugin: `npm run build:backend`
- Run backend tests: `mage test` (see mage targets for more options)

## Building & Testing (summary)

- Build frontend: `npm run build`
- Build backend: `mage buildAll`
- Frontend tests: `npm run test:ci`
- Backend tests: `mage testRace`
- E2E tests: `npm run e2e -- --retries=3`

## Linting

- Frontend: `npm run lint` (ESLint)
- Frontend formatting: `npm run lint:fix` (calls Prettier)
- Backend: `golangci-lint`
- We provide pre-commit hooks that run ESLint, Prettier and `golangci-lint` on commits — ensure `pre-commit install` is run once after cloning.

## Code Style

- Follow existing project conventions and ESLint rules.
- Keep commits focused and add tests for new behaviours.

## Reporting Issues

- Use GitHub Issues for bugs, feature requests, and questions.

See [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) for community guidelines.


# Contributing to Kafka Datasource for Grafana

Thank you for considering contributing! We welcome issues, feature requests, and pull requests.

## How to Contribute

1. **Fork the repository** and create your branch from `main` or the relevant feature branch.
2. **Open an issue** to discuss bugs, features, or questions before starting major work.
3. **Submit a pull request** with a clear description of your changes.

## Developer Setup

### Dev Container (Recommended)

This project includes a pre-configured [Dev Container](https://containers.dev/) for a consistent development environment. It installs Node.js, Go, Mage, Playwright, and other dependencies automatically.

**How to use:**

1. Open the project in [VS Code](https://code.visualstudio.com/) with the [Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers).
2. When prompted, "Reopen in Container" to start the dev container.
3. All tools and dependencies will be installed automatically.
4. You can run all `npm`, `go`, and `mage` commands inside the container terminal.

**Notes:**
- The dev container uses host network mode for better compatibility with local services.
- Playwright browsers are pre-installed for the non-root user; E2E tests work out of the box.
- If you update dependencies in `Dockerfile`, rebuild the container from the command palette: `Dev Containers: Rebuild Container`.

---

### Prerequisites

- Grafana v10.2+
- Node.js v22.15+
- Go 1.24.1+
- Mage v1.15.0+
- Docker

### Frontend (React)

1. Install dependencies:
	```bash
	npm install
	```
2. Build and run in development mode:
	```bash
	npm run dev
	```
3. Build for production:
	```bash
	npm run build
	```
4. Run unit tests:
	```bash
	npm run test
	npm run test:ci
	```
5. Run E2E tests (Playwright):
	```bash
	npm run server
	npm run e2e
	```
6. Lint code:
	```bash
	npm run lint
	npm run lint:fix
	```

### Backend (Golang)

1. Update plugin SDK:
	```bash
	go get -u github.com/grafana/grafana-plugin-sdk-go
	go mod tidy
	```
2. Build backend plugin:
	```bash
	npm run build:backend
	```
3. Test backend plugin:
	```bash
	mage test
	```

## Building & Testing

- Use `mage` for backend builds and platform targets.
- Use `npm` scripts for frontend builds, tests, and linting.
- See [README.md](README.md) for usage/configuration.

## Code Style

- Follow existing code conventions.
- Write clear commit messages.
- Add/maintain tests for new features and bug fixes.

## Reporting Issues

- Use GitHub Issues for bugs, feature requests, and questions.

---

See [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) for community guidelines.

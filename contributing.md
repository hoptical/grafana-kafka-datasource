
# Contributing to Kafka Datasource for Grafana

Thank you for considering contributing! We welcome issues, feature requests, and pull requests.

## How to Contribute

1. **Fork the repository** and create your branch from `main` or the relevant feature branch.
2. **Open an issue** to discuss bugs, features, or questions before starting major work.
3. **Submit a pull request** with a clear description of your changes.

## Developer Setup

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

See [code_of_conduct.md](code_of_conduct.md) for community guidelines.

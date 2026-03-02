# Contributing

## Prerequisites

- **Go 1.25+** — [install](https://go.dev/dl/)
- **Docker** — required for integration and E2E tests
- **Bun** — required for E2E tests ([install](https://bun.sh))
- **golangci-lint** — for linting ([install](https://golangci-lint.run/usage/install/))

## Clone

The `multigres` submodule is required for building:

```bash
git clone --recurse-submodules https://github.com/avallete/supa-proxy.git
cd supa-proxy
```

If you already cloned without `--recurse-submodules`:

```bash
git submodule update --init --recursive
```

## Build & Test

All commands run from `pg-gateway/`:

```bash
cd pg-gateway

make build              # Build binary
make test               # Unit tests (no Docker)
make test-integration   # Integration tests (Docker required)
make test-e2e           # Bun E2E suite (Docker + Bun required)
make bench              # Benchmarks
make lint               # golangci-lint
make fmt                # go fmt
make vet                # go vet
```

## Code Style

- `go fmt` for formatting
- `go vet` for static analysis
- `golangci-lint run` for comprehensive linting
- Keep changes focused — don't refactor unrelated code

## Docker

```bash
cd pg-gateway

# Two-container mode (Postgres + pg-gateway sidecar)
make docker-up
make docker-down

# Combined mode (Postgres + pg-gateway in one image)
docker compose -f docker-compose.combined.yml up --build
```

## PR Workflow

1. Fork the repo
2. Create a feature branch: `git checkout -b feat/my-feature`
3. Make your changes
4. Run tests: `make test && make lint`
5. Open a pull request against `main`

# supa-proxy

[![CI](https://github.com/avallete/supa-proxy/actions/workflows/ci.yml/badge.svg)](https://github.com/avallete/supa-proxy/actions/workflows/ci.yml)
[![Docker](https://github.com/avallete/supa-proxy/actions/workflows/docker.yml/badge.svg)](https://github.com/avallete/supa-proxy/actions/workflows/docker.yml)

A high-performance PostgreSQL proxy with JWT authentication, credential injection, and streaming JSON SQL endpoint.

## Project Structure

```
supa-proxy/
├── pg-gateway/          # Go proxy binary (main package)
│   ├── e2e/             # Bun E2E test suite
│   ├── demo/            # Live demo (Bun + TypeScript)
│   ├── Dockerfile       # Standalone proxy image
│   ├── Dockerfile.postgres  # Combined PG + proxy image
│   └── README.md        # Full API documentation
├── multigres/           # Git submodule — shared PG utilities
└── .github/workflows/   # CI/CD pipelines
```

## Quick Start

```bash
# Clone with submodules (required — multigres is a submodule)
git clone --recurse-submodules https://github.com/avallete/supa-proxy.git
cd supa-proxy/pg-gateway

# Start with Docker Compose (Postgres + pg-gateway)
docker compose up --build

# Verify
curl http://localhost:15432/health
```

## Documentation

- **[pg-gateway/README.md](pg-gateway/README.md)** — Full API docs, configuration reference, architecture
- **[CONTRIBUTING.md](CONTRIBUTING.md)** — Development workflow, prerequisites, testing guide

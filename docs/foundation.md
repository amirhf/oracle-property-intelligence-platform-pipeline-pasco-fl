# Executable Oracle foundation

This checkpoint establishes only the local mutation-plane foundation. It does not download,
ingest, publish, deploy, or expose real Pasco records.

## Frozen topology

- Node.js `22.23.2` runs the Restate endpoint directly on the host at `9080`.
- Docker Compose contains only PostgreSQL 16 and Restate 1.7.2.
- PostgreSQL maps `127.0.0.1:5433` to container port `5432`.
- Restate maps `127.0.0.1:8080` and `127.0.0.1:9070` and keeps node identity `restate-1`.
- Host port `5432` is not used or managed by this project.

## Local setup

1. Use Node `22.23.2` and pnpm `11.19.0`.
2. Run `pnpm install --frozen-lockfile` after the lockfile exists.
3. Create `data/seeds` and `data/artifacts`; generated data is ignored by Git.
4. Copy `.env.example` to an ignored `.env` and replace `DATA_DIR` with this repository's
   absolute `data` path, or export the three required variables in the shell.
5. Start PostgreSQL and Restate with `docker compose up -d`.
6. Start the host endpoint with `pnpm start`.
7. Register it with
   `pnpm exec restate deployments register http://host.docker.internal:9080`.
8. Run `pnpm health:check` to verify PostgreSQL, Restate, the host listener, and the
   registered `Parcel/health` database round trip.

The `Parcel` service currently exposes only the foundation health handler. County capture,
transformation, loading, enrichment, publication, MCP, and explorer behavior remain outside
this checkpoint.

## Verification

Run `pnpm format:check`, `pnpm typecheck`, `pnpm test`, and `pnpm contracts:validate`.
Contract validation compiles the frozen MCP schema with Ajv 8 draft 2020-12 support,
validates all five fixtures against their named definitions, runs negative mutation tests,
and verifies every hash recorded in `contracts/contract-lock.json` without rewriting any
contract file.

#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

echo "[freshann] starting baseline containers"
docker compose -f test/bench/docker-compose.baselines.yml up -d

echo "[freshann] run benchmark harness"
go test ./pkg/bench -run TestRunComparisonAndThreshold -v

echo "[freshann] baseline stack is up. tear down with:"
echo "docker compose -f test/bench/docker-compose.baselines.yml down -v"

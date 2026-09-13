#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

rm -f resources/08_ingestion_materialize_landing.yml
rm -f resources/10_ingestion_materialize_object.yml
rm -f src/ingestion/materialize_landing_run.py
find src -type d -name '__pycache__' -prune -exec rm -rf {} +
find src -type f -name '*.pyc' -delete

echo "Artefatos obsoletos removidos."

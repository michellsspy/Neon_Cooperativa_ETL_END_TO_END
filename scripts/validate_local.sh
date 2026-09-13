#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "[1/4] Compilando scripts Python..."
python3 -m compileall -q src

echo "[2/4] Validando compatibilidade Serverless/Spark Connect..."
if grep -RInE '\.cache\(|\.persist\(|\.unpersist\(|\.checkpoint\(|errorifexists|time\.sleep' src/ingestion src/preflight; then
  echo "ERRO: padrão incompatível/obsoleto encontrado nos executores Serverless." >&2
  exit 1
fi

echo "[3/4] Validando referências python_file dos recursos..."
python3 - <<'PY'
from pathlib import Path
import re

root = Path.cwd()
errors = []
for resource in sorted((root / "resources").glob("*.yml")):
    text = resource.read_text(encoding="utf-8")
    for match in re.finditer(r"^\s*python_file:\s*(\S+)\s*$", text, flags=re.MULTILINE):
        raw = match.group(1).strip('"\'')
        candidate = (resource.parent / raw).resolve()
        if not candidate.is_file():
            errors.append(f"{resource.relative_to(root)} -> {raw}")

if errors:
    raise SystemExit("Referências inválidas:\n  " + "\n  ".join(errors))

obsolete = [
    root / "resources/08_ingestion_materialize_landing.yml",
    root / "src/ingestion/materialize_landing_run.py",
]
found = [str(p.relative_to(root)) for p in obsolete if p.exists()]
if found:
    raise SystemExit("Artefatos obsoletos ainda presentes: " + ", ".join(found))

print("Referências de arquivos OK.")
PY

echo "[4/4] Validação local concluída."
echo "Próximo gate: databricks bundle validate -t dev"

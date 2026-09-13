# Revisão técnica — 2026-09-13

## Correções aplicadas

1. Removido o recurso legado `ingestion_materialize_landing`, que executava um run inteiro dentro de um único script.
2. Padronizado o executor `materialize_landing_object.py`: um `run_object_id` por execução.
3. Eliminado drift entre YAML e arquivo Python.
4. Mantida compatibilidade Serverless/Spark Connect: sem `cache`, `persist`, `unpersist` e `checkpoint`.
5. Padronizado `mode("error")` para proteção de imutabilidade da Landing.
6. Planner não ressuscita runs `FAILED`.
7. `run_id` não depende de `repair_count`; repairs da mesma execução Lakeflow reutilizam o mesmo run lógico.
8. Concurrency guard considera também o status do run pai; runs terminais não bloqueiam novos planos.
9. `MERGE ... INSERT *` removido do planner em favor de colunas explícitas.
10. `source_system.connection_name` passou a ser configurado por Bundle (`source_connection_name`).
11. Adicionado gate local `scripts/validate_local.sh` para detectar referências quebradas e APIs incompatíveis antes do deploy.
12. ZIP de entrega não inclui `.git`, `.databricks`, `venv` ou `__pycache__`.

## Artefatos preservados

As estruturas Controller já validadas permanecem compatíveis:

- `source_system`
- `source_object`
- `source_column`
- `incremental_profile`
- `ingestion_policy`
- `ingestion_state`
- `ingestion_run`
- `ingestion_run_object`

## Próxima fronteira

Depois de validar um novo run e materializar um único objeto com sucesso, a próxima evolução deve orquestrar múltiplos `run_object_id` e, posteriormente, implementar reconciliação formal e commit otimista de `ingestion_state`.

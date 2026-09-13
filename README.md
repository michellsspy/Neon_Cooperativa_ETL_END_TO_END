# Neon Cooperativa ETL End-to-End

Laboratório de engenharia de dados em Databricks com Lakeflow Jobs, PySpark, Delta Lake e Unity Catalog. A arquitetura segue responsabilidades explícitas de Landing, Controller, Bronze, Quarentena, Silver Técnica, Silver Analítica e Gold. O estágio implementado neste repositório vai até o planejamento e a materialização controlada da Landing.

## Arquitetura implementada

```text
PostgreSQL Neon
    │
    ▼
Foreign Catalog: neon_fenix_cooperativa
    │
    ▼
Controller (Unity Catalog / Delta)
    ├── source_system
    ├── source_object
    ├── source_column
    ├── incremental_profile
    ├── ingestion_policy
    ├── ingestion_state
    ├── ingestion_run
    └── ingestion_run_object
    │
    ▼
Landing Volume
/Volumes/neon_cooperativa_dev/landing/raw/...
```

A Landing é append-only por tentativa. Não há deduplicação na Landing. O `ingestion_state` só poderá avançar após materialização, reconciliação e commit de estado em etapas posteriores.

## Pré-requisito de plataforma

O catálogo gerenciado `neon_cooperativa_dev` deve existir antes do deploy do Bundle. O Bundle gerencia os schemas e o Volume dentro desse catálogo.

## Convenções operacionais

- `ingestion_policy`: configuração declarativa.
- `ingestion_state`: último checkpoint confirmado.
- `ingestion_run`: execução lógica.
- `ingestion_run_object`: tentativa por objeto.
- `run_id`: estável durante repairs da mesma execução Lakeflow.
- `run_object_id`: determinístico por `run_id + source_object_key + attempt_number`.
- caminhos Landing incluem `extraction_date`, `run_id` e `attempt_number`.
- executor Serverless não usa `cache`, `persist`, `unpersist` ou `checkpoint`.
- escrita imutável usa `DataFrameWriter.mode("error")`.

## Atualização sobre um checkout existente

Se este pacote for copiado por cima de uma versão anterior do projeto, execute primeiro:

```bash
./scripts/cleanup_obsolete.sh
```

Isso remove os artefatos legados que não devem coexistir com o executor por objeto.

## Validação local

```bash
./scripts/validate_local.sh

databricks bundle validate -t dev
```

## Bootstrap do Controller

Executar na ordem:

```bash
databricks bundle deploy -t dev

databricks bundle run controller_bootstrap -t dev
databricks bundle run controller_seed -t dev
databricks bundle run controller_profile_incremental -t dev
databricks bundle run controller_seed_ingestion_policy -t dev
databricks bundle run controller_bootstrap_ingestion_state -t dev
databricks bundle run controller_bootstrap_ingestion_run_model -t dev
```

## Preflight Serverless

O preflight valida Spark Connect, `Observation`, Parquet em Unity Catalog Volume, read-back, manifesto JSON e proteção contra overwrite.

```bash
databricks bundle run serverless_landing_preflight -t dev
```

## Planejamento

```bash
databricks bundle run ingestion_plan -t dev
```

O planner cria o `ingestion_run` e os `ingestion_run_object`, congela modo de carga, bounds e `state_version_before`, mas não grava a Landing e não altera `ingestion_state`.

## Materialização de um objeto

```bash
databricks bundle run -t dev \
  --params run_object_id=<RUN_OBJECT_ID> \
  ingestion_materialize_object
```

O executor processa exatamente uma tentativa. Ele não cria retry, não recalcula bounds e não avança watermark.

## Estado atual

O fluxo estabilizado é:

```text
PLAN -> MATERIALIZE ONE OBJECT -> [futuro] RECONCILE -> [futuro] COMMIT STATE
```

Reconcile e commit de `ingestion_state` ainda não fazem parte desta versão.

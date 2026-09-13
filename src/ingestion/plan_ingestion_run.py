import argparse
import re
import uuid
from datetime import date, datetime, timedelta, timezone

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


CONTROL_SCHEMA = "control"
RUN_TABLE = "ingestion_run"
RUN_OBJECT_TABLE = "ingestion_run_object"

ACTIVE_PARENT_RUN_STATUSES = ("CREATED", "RUNNING")
ACTIVE_RUN_OBJECT_STATUSES = (
    "PENDING",
    "RUNNING",
    "MATERIALIZED",
    "RECONCILED",
)
TERMINAL_PARENT_RUN_STATUSES = (
    "SUCCEEDED",
    "PARTIAL",
    "FAILED",
    "CANCELLED",
)


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Cria um plano imutável de ingestão a partir do Controller. "
            "O planner não grava dados na Landing e não altera ingestion_state."
        )
    )
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schedule-group", required=True)
    parser.add_argument("--requested-by", required=True)
    parser.add_argument("--orchestrator-job-id", required=True)
    parser.add_argument("--orchestrator-run-id", required=True)
    parser.add_argument("--repair-count", required=True, type=int)
    parser.add_argument("--orchestrator-trigger-type", required=True)
    return parser.parse_args()


def validate_identifier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_]+", value):
        raise ValueError(f"Identificador inválido: {value}")
    return value


def validate_schedule_group(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_-]+", value):
        raise ValueError(f"schedule_group inválido: {value}")
    return value


def escape_sql_literal(value: str) -> str:
    return value.replace("'", "''")


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def validate_source_fqn(value: str) -> str:
    parts = value.split(".")
    if len(parts) != 3:
        raise ValueError(f"source_fqn inválido: {value}")
    for part in parts:
        validate_identifier(part)
    return value


def quote_source_fqn(value: str) -> str:
    validate_source_fqn(value)
    catalog, schema, table = value.split(".")
    return f"`{catalog}`.`{schema}`.`{table}`"


def serialize_watermark(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat(timespec="microseconds")
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def map_trigger_type(orchestrator_trigger_type: str) -> str:
    normalized = orchestrator_trigger_type.strip().lower()
    mapping = {
        "one_time": "MANUAL",
        "manual": "MANUAL",
        "periodic": "SCHEDULED",
        "scheduled": "SCHEDULED",
        "run_job_task": "SCHEDULED",
        "file_arrival": "SCHEDULED",
        "continuous": "SCHEDULED",
        "table": "SCHEDULED",
        "model": "SCHEDULED",
    }
    if normalized not in mapping:
        raise ValueError(
            "Tipo de trigger do orquestrador não suportado: "
            f"{orchestrator_trigger_type}"
        )
    return mapping[normalized]


def generate_run_id(
    orchestrator_job_id: str,
    orchestrator_run_id: str,
) -> str:
    """
    O run lógico é estável durante repairs da mesma execução Lakeflow.
    repair_count pertence à auditoria do trigger, não à identidade do run.
    """
    deterministic_key = (
        f"databricks:job:{orchestrator_job_id}:run:{orchestrator_run_id}"
    )
    return str(uuid.uuid5(uuid.NAMESPACE_URL, deterministic_key))


def generate_run_object_id(
    run_id: str,
    source_object_key: str,
    attempt_number: int,
) -> str:
    deterministic_key = (
        f"{run_id}:{source_object_key}:attempt:{attempt_number}"
    )
    return str(uuid.uuid5(uuid.NAMESPACE_URL, deterministic_key))


def discover_objects(
    spark: SparkSession,
    catalog: str,
    schedule_group: str,
):
    schedule_filter = ""
    if schedule_group.upper() != "ALL":
        schedule_filter = (
            "AND ip.schedule_group = "
            f"'{escape_sql_literal(schedule_group)}'"
        )

    rows = spark.sql(
        f"""
        SELECT
            so.source_object_key,
            so.source_table,
            so.source_fqn,
            ip.bootstrap_mode,
            ip.steady_state_mode,
            ip.incremental_column,
            ip.watermark_data_type,
            ip.lookback_days,
            ip.schedule_group,
            st.bootstrap_completed,
            st.last_successful_watermark,
            st.state_version
        FROM `{catalog}`.`{CONTROL_SCHEMA}`.`source_object` AS so
        INNER JOIN `{catalog}`.`{CONTROL_SCHEMA}`.`ingestion_policy` AS ip
            ON ip.source_object_key = so.source_object_key
        INNER JOIN `{catalog}`.`{CONTROL_SCHEMA}`.`ingestion_state` AS st
            ON st.source_object_key = so.source_object_key
        WHERE so.enabled = TRUE
          AND ip.enabled = TRUE
          {schedule_filter}
        ORDER BY so.source_object_key
        """
    ).collect()

    if not rows:
        raise RuntimeError("Nenhum objeto elegível foi encontrado para o planejamento.")
    return rows


def resolve_load_mode(
    bootstrap_completed: bool,
    bootstrap_mode: str,
    steady_state_mode: str,
) -> str:
    return steady_state_mode if bootstrap_completed else bootstrap_mode


def get_source_upper_bound(
    spark: SparkSession,
    source_fqn: str,
    cursor_column: str,
):
    validate_identifier(cursor_column)
    source = quote_source_fqn(source_fqn)
    return spark.sql(
        f"SELECT MAX(`{cursor_column}`) AS upper_bound FROM {source}"
    ).first()["upper_bound"]


def compute_lower_bound(
    last_successful_watermark: str,
    watermark_data_type: str,
    lookback_days: int,
) -> str:
    if last_successful_watermark is None:
        raise RuntimeError(
            "Execução incremental solicitada sem last_successful_watermark."
        )

    normalized_type = watermark_data_type.strip().upper()
    if normalized_type == "TIMESTAMP":
        value = datetime.fromisoformat(last_successful_watermark)
        return (value - timedelta(days=lookback_days)).isoformat(
            timespec="microseconds"
        )
    if normalized_type == "DATE":
        value = date.fromisoformat(last_successful_watermark)
        return (value - timedelta(days=lookback_days)).isoformat()

    raise RuntimeError(
        "Tipo de watermark não suportado pelo planner: "
        f"{watermark_data_type}"
    )


def validate_watermark_progression(
    last_successful_watermark: str | None,
    upper_bound,
    watermark_data_type: str,
):
    if last_successful_watermark is None or upper_bound is None:
        return

    normalized_type = watermark_data_type.strip().upper()
    if normalized_type == "TIMESTAMP":
        previous = datetime.fromisoformat(last_successful_watermark)
    elif normalized_type == "DATE":
        previous = date.fromisoformat(last_successful_watermark)
    else:
        raise RuntimeError(
            "Tipo de watermark não suportado na validação: "
            f"{watermark_data_type}"
        )

    if upper_bound < previous:
        raise RuntimeError(
            "Regressão de watermark detectada. "
            f"Estado anterior={previous}, origem atual={upper_bound}"
        )


def get_existing_run_status(
    spark: SparkSession,
    catalog: str,
    run_id: str,
):
    rows = spark.sql(
        f"""
        SELECT status
        FROM `{catalog}`.`{CONTROL_SCHEMA}`.`{RUN_TABLE}`
        WHERE run_id = '{escape_sql_literal(run_id)}'
        """
    ).collect()
    if len(rows) > 1:
        raise RuntimeError(f"run_id duplicado em ingestion_run: {run_id}")
    return None if not rows else rows[0]["status"]


def ensure_ingestion_run(
    spark: SparkSession,
    catalog: str,
    run_id: str,
    trigger_type: str,
    trigger_reference: str,
    schedule_group: str,
    requested_by: str,
    orchestrator_job_id: str,
    orchestrator_run_id: str,
):
    """
    Cria o envelope uma única vez. Runs terminais nunca são ressuscitados.
    """
    existing_status = get_existing_run_status(spark, catalog, run_id)
    if existing_status in TERMINAL_PARENT_RUN_STATUSES:
        raise RuntimeError(
            "O run lógico já está em estado terminal e não pode ser reaberto: "
            f"run_id={run_id}, status={existing_status}"
        )
    if existing_status in ACTIVE_PARENT_RUN_STATUSES:
        print(
            f"ingestion_run já existe em status {existing_status}; "
            "será reutilizado de forma idempotente."
        )
        return

    now = utc_now()
    schema = StructType(
        [
            StructField("run_id", StringType(), False),
            StructField("trigger_type", StringType(), False),
            StructField("trigger_reference", StringType(), True),
            StructField("schedule_group", StringType(), True),
            StructField("requested_by", StringType(), True),
            StructField("orchestrator_job_id", StringType(), True),
            StructField("orchestrator_run_id", StringType(), True),
            StructField("status", StringType(), False),
            StructField("started_at", TimestampType(), False),
            StructField("completed_at", TimestampType(), True),
            StructField("created_at", TimestampType(), False),
            StructField("updated_at", TimestampType(), False),
        ]
    )
    spark.createDataFrame(
        [
            (
                run_id,
                trigger_type,
                trigger_reference,
                schedule_group,
                requested_by,
                orchestrator_job_id,
                orchestrator_run_id,
                "CREATED",
                now,
                None,
                now,
                now,
            )
        ],
        schema=schema,
    ).createOrReplaceTempView("planned_ingestion_run")

    spark.sql(
        f"""
        MERGE INTO `{catalog}`.`{CONTROL_SCHEMA}`.`{RUN_TABLE}` AS tgt
        USING planned_ingestion_run AS src
        ON tgt.run_id = src.run_id
        WHEN NOT MATCHED THEN INSERT (
            run_id,
            trigger_type,
            trigger_reference,
            schedule_group,
            requested_by,
            orchestrator_job_id,
            orchestrator_run_id,
            status,
            started_at,
            completed_at,
            created_at,
            updated_at
        ) VALUES (
            src.run_id,
            src.trigger_type,
            src.trigger_reference,
            src.schedule_group,
            src.requested_by,
            src.orchestrator_job_id,
            src.orchestrator_run_id,
            src.status,
            src.started_at,
            src.completed_at,
            src.created_at,
            src.updated_at
        )
        """
    )


def existing_plan_count(
    spark: SparkSession,
    catalog: str,
    run_id: str,
) -> int:
    return int(
        spark.sql(
            f"""
            SELECT COUNT(*) AS qtd
            FROM `{catalog}`.`{CONTROL_SCHEMA}`.`{RUN_OBJECT_TABLE}`
            WHERE run_id = '{escape_sql_literal(run_id)}'
              AND attempt_number = 1
            """
        ).first()["qtd"]
    )


def validate_no_competing_runs(
    spark: SparkSession,
    catalog: str,
    run_id: str,
    objects,
):
    """
    Considera concorrente somente um objeto pertencente a run pai ativo.
    Runs FAILED/CANCELLED/SUCCEEDED/PARTIAL não bloqueiam novos planos.
    """
    object_filter = ", ".join(
        f"'{escape_sql_literal(row['source_object_key'])}'" for row in objects
    )
    object_status_filter = ", ".join(
        f"'{value}'" for value in ACTIVE_RUN_OBJECT_STATUSES
    )
    parent_status_filter = ", ".join(
        f"'{value}'" for value in ACTIVE_PARENT_RUN_STATUSES
    )

    competing = spark.sql(
        f"""
        SELECT
            ro.run_id,
            ro.source_object_key,
            ro.status AS object_status,
            r.status AS run_status
        FROM `{catalog}`.`{CONTROL_SCHEMA}`.`{RUN_OBJECT_TABLE}` AS ro
        INNER JOIN `{catalog}`.`{CONTROL_SCHEMA}`.`{RUN_TABLE}` AS r
            ON r.run_id = ro.run_id
        WHERE ro.source_object_key IN ({object_filter})
          AND ro.status IN ({object_status_filter})
          AND r.status IN ({parent_status_filter})
          AND ro.run_id <> '{escape_sql_literal(run_id)}'
        """
    ).collect()

    if competing:
        details = "; ".join(
            (
                f"{row['source_object_key']} run={row['run_id']} "
                f"run_status={row['run_status']} "
                f"object_status={row['object_status']}"
            )
            for row in competing
        )
        raise RuntimeError(
            "Existem execuções concorrentes ainda ativas: " + details
        )


def build_run_object_rows(
    spark: SparkSession,
    run_id: str,
    objects,
):
    now = utc_now()
    rows = []

    for obj in objects:
        load_mode = resolve_load_mode(
            bool(obj["bootstrap_completed"]),
            obj["bootstrap_mode"],
            obj["steady_state_mode"],
        )

        watermark_column = None
        watermark_lower_bound = None
        watermark_upper_bound = None
        lookback_days_applied = None

        if obj["steady_state_mode"] == "INCREMENTAL":
            watermark_column = obj["incremental_column"]
            if not watermark_column:
                raise RuntimeError(
                    f"Objeto incremental sem incremental_column: {obj['source_table']}"
                )

            upper_bound = get_source_upper_bound(
                spark,
                obj["source_fqn"],
                watermark_column,
            )
            watermark_upper_bound = serialize_watermark(upper_bound)

            if load_mode == "INCREMENTAL":
                lookback_days_applied = int(obj["lookback_days"] or 0)
                watermark_lower_bound = compute_lower_bound(
                    obj["last_successful_watermark"],
                    obj["watermark_data_type"],
                    lookback_days_applied,
                )
                validate_watermark_progression(
                    obj["last_successful_watermark"],
                    upper_bound,
                    obj["watermark_data_type"],
                )

        attempt_number = 1
        run_object_id = generate_run_object_id(
            run_id,
            obj["source_object_key"],
            attempt_number,
        )

        rows.append(
            (
                run_object_id,
                run_id,
                obj["source_object_key"],
                attempt_number,
                None,
                load_mode,
                "PENDING",
                watermark_column,
                watermark_lower_bound,
                watermark_upper_bound,
                lookback_days_applied,
                None,
                None,
                None,
                None,
                int(obj["state_version"]),
                None,
                False,
                None,
                None,
                None,
                None,
                now,
                now,
            )
        )

    return rows


def persist_run_objects(
    spark: SparkSession,
    catalog: str,
    rows,
):
    schema = StructType(
        [
            StructField("run_object_id", StringType(), False),
            StructField("run_id", StringType(), False),
            StructField("source_object_key", StringType(), False),
            StructField("attempt_number", IntegerType(), False),
            StructField("reprocess_of_run_object_id", StringType(), True),
            StructField("load_mode", StringType(), False),
            StructField("status", StringType(), False),
            StructField("watermark_column", StringType(), True),
            StructField("watermark_lower_bound", StringType(), True),
            StructField("watermark_upper_bound", StringType(), True),
            StructField("lookback_days_applied", IntegerType(), True),
            StructField("source_row_count", LongType(), True),
            StructField("landing_row_count", LongType(), True),
            StructField("landing_data_path", StringType(), True),
            StructField("manifest_path", StringType(), True),
            StructField("state_version_before", LongType(), True),
            StructField("state_version_after", LongType(), True),
            StructField("state_committed", BooleanType(), False),
            StructField("error_class", StringType(), True),
            StructField("error_message", StringType(), True),
            StructField("started_at", TimestampType(), True),
            StructField("completed_at", TimestampType(), True),
            StructField("created_at", TimestampType(), False),
            StructField("updated_at", TimestampType(), False),
        ]
    )
    spark.createDataFrame(rows, schema=schema).createOrReplaceTempView(
        "planned_ingestion_run_objects"
    )

    spark.sql(
        f"""
        MERGE INTO `{catalog}`.`{CONTROL_SCHEMA}`.`{RUN_OBJECT_TABLE}` AS tgt
        USING planned_ingestion_run_objects AS src
        ON tgt.run_object_id = src.run_object_id
        WHEN NOT MATCHED THEN INSERT (
            run_object_id,
            run_id,
            source_object_key,
            attempt_number,
            reprocess_of_run_object_id,
            load_mode,
            status,
            watermark_column,
            watermark_lower_bound,
            watermark_upper_bound,
            lookback_days_applied,
            source_row_count,
            landing_row_count,
            landing_data_path,
            manifest_path,
            state_version_before,
            state_version_after,
            state_committed,
            error_class,
            error_message,
            started_at,
            completed_at,
            created_at,
            updated_at
        ) VALUES (
            src.run_object_id,
            src.run_id,
            src.source_object_key,
            src.attempt_number,
            src.reprocess_of_run_object_id,
            src.load_mode,
            src.status,
            src.watermark_column,
            src.watermark_lower_bound,
            src.watermark_upper_bound,
            src.lookback_days_applied,
            src.source_row_count,
            src.landing_row_count,
            src.landing_data_path,
            src.manifest_path,
            src.state_version_before,
            src.state_version_after,
            src.state_committed,
            src.error_class,
            src.error_message,
            src.started_at,
            src.completed_at,
            src.created_at,
            src.updated_at
        )
        """
    )


def validate_plan(
    spark: SparkSession,
    catalog: str,
    run_id: str,
    expected_count: int,
):
    row = spark.sql(
        f"""
        SELECT
            COUNT(*) AS total,
            COUNT(DISTINCT source_object_key) AS distinct_objects
        FROM `{catalog}`.`{CONTROL_SCHEMA}`.`{RUN_OBJECT_TABLE}`
        WHERE run_id = '{escape_sql_literal(run_id)}'
          AND attempt_number = 1
        """
    ).first()

    total = int(row["total"] or 0)
    distinct_objects = int(row["distinct_objects"] or 0)
    if total != expected_count or distinct_objects != expected_count:
        raise RuntimeError(
            "Plano inconsistente. "
            f"esperado={expected_count}, total={total}, "
            f"objetos_distintos={distinct_objects}"
        )


def print_plan(
    spark: SparkSession,
    catalog: str,
    run_id: str,
):
    rows = spark.sql(
        f"""
        SELECT
            so.source_table,
            ro.run_object_id,
            ro.load_mode,
            ro.watermark_column,
            ro.watermark_lower_bound,
            ro.watermark_upper_bound,
            ro.lookback_days_applied,
            ro.state_version_before,
            ro.status
        FROM `{catalog}`.`{CONTROL_SCHEMA}`.`{RUN_OBJECT_TABLE}` AS ro
        INNER JOIN `{catalog}`.`{CONTROL_SCHEMA}`.`source_object` AS so
            ON so.source_object_key = ro.source_object_key
        WHERE ro.run_id = '{escape_sql_literal(run_id)}'
          AND ro.attempt_number = 1
        ORDER BY so.source_table
        """
    ).collect()

    full_count = sum(1 for row in rows if row["load_mode"] == "FULL")
    incremental_count = sum(
        1 for row in rows if row["load_mode"] == "INCREMENTAL"
    )

    print("=" * 60)
    print(f"run_id: {run_id}")
    print(f"objetos planejados: {len(rows)}")
    print(f"FULL........: {full_count}")
    print(f"INCREMENTAL.: {incremental_count}")
    print("-" * 60)
    for row in rows:
        print(
            f"{row['source_table']}: mode={row['load_mode']} | "
            f"lower={row['watermark_lower_bound']} | "
            f"upper={row['watermark_upper_bound']} | "
            f"state_version={row['state_version_before']} | "
            f"run_object_id={row['run_object_id']}"
        )
    print("=" * 60)


def main():
    args = parse_args()
    catalog = validate_identifier(args.catalog)
    schedule_group = validate_schedule_group(args.schedule_group)
    requested_by = args.requested_by.strip()
    if not requested_by:
        raise ValueError("requested_by não pode ser vazio.")

    trigger_type = map_trigger_type(args.orchestrator_trigger_type)
    run_id = generate_run_id(
        args.orchestrator_job_id,
        args.orchestrator_run_id,
    )
    trigger_reference = (
        f"databricks:job={args.orchestrator_job_id};"
        f"run={args.orchestrator_run_id};repair={args.repair_count}"
    )

    spark = SparkSession.builder.getOrCreate()

    print("=" * 60)
    print("Planejamento da execução de ingestão")
    print(f"run_id.........: {run_id}")
    print(f"schedule_group.: {schedule_group}")
    print(f"trigger_type...: {trigger_type}")
    print(f"repair_count...: {args.repair_count}")
    print("=" * 60)

    objects = discover_objects(spark, catalog, schedule_group)
    current_plan_count = existing_plan_count(spark, catalog, run_id)

    if current_plan_count == 0:
        validate_no_competing_runs(spark, catalog, run_id, objects)

        # Calcula o plano completo antes de persistir o envelope. Assim,
        # uma falha de leitura da origem não deixa um run vazio no ledger.
        run_object_rows = build_run_object_rows(spark, run_id, objects)

        ensure_ingestion_run(
            spark,
            catalog,
            run_id,
            trigger_type,
            trigger_reference,
            schedule_group,
            requested_by,
            args.orchestrator_job_id,
            args.orchestrator_run_id,
        )
        persist_run_objects(spark, catalog, run_object_rows)
    else:
        ensure_ingestion_run(
            spark,
            catalog,
            run_id,
            trigger_type,
            trigger_reference,
            schedule_group,
            requested_by,
            args.orchestrator_job_id,
            args.orchestrator_run_id,
        )
        print(
            "Plano attempt_number=1 já existe para este run_id; "
            "os bounds congelados serão reutilizados."
        )

    validate_plan(spark, catalog, run_id, len(objects))
    print_plan(spark, catalog, run_id)
    print("Planejamento concluído com sucesso.")
    print("Nenhum dado foi escrito na Landing.")
    print("ingestion_state não foi alterado.")


if __name__ == "__main__":
    main()

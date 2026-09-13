import argparse
import re
import uuid
from datetime import datetime, timezone

from pyspark.sql import Observation, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
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
STATE_TABLE = "ingestion_state"
SUPPORTED_LANDING_FORMATS = {"PARQUET"}


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Materializa exatamente um run_object previamente planejado. "
            "Não cria retry, não recalcula bounds e não altera ingestion_state."
        )
    )
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--run-object-id", required=True)
    return parser.parse_args()


def validate_identifier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_]+", value):
        raise ValueError(f"Identificador inválido: {value}")
    return value


def validate_uuid(value: str) -> str:
    try:
        return str(uuid.UUID(value))
    except ValueError as exc:
        raise ValueError(f"UUID inválido: {value}") from exc


def validate_source_fqn(value: str) -> str:
    parts = value.split(".")
    if len(parts) != 3:
        raise ValueError(f"source_fqn inválido: {value}")
    for part in parts:
        validate_identifier(part)
    return value


def validate_relative_path(value: str) -> str:
    if not value or value.startswith("/"):
        raise ValueError("landing_relative_path deve ser relativo e não vazio.")
    if ".." in value.split("/"):
        raise ValueError("landing_relative_path contém '..'.")
    if not re.fullmatch(r"[A-Za-z0-9_./-]+", value):
        raise ValueError(
            "landing_relative_path contém caracteres não permitidos: "
            f"{value}"
        )
    return value.strip("/")


def escape_sql_literal(value: str) -> str:
    return value.replace("'", "''")


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def get_run_object_context(
    spark: SparkSession,
    catalog: str,
    run_object_id: str,
):
    rows = spark.sql(
        f"""
        SELECT
            ro.run_object_id,
            ro.run_id,
            ro.source_object_key,
            ro.attempt_number,
            ro.reprocess_of_run_object_id,
            ro.load_mode,
            ro.status,
            ro.watermark_column,
            ro.watermark_lower_bound,
            ro.watermark_upper_bound,
            ro.lookback_days_applied,
            ro.state_version_before,
            ro.state_version_after,
            ro.state_committed,
            r.status AS run_status,
            r.started_at AS run_started_at,
            so.source_table,
            so.source_fqn,
            so.landing_catalog,
            so.landing_schema,
            so.landing_volume,
            so.landing_relative_path,
            ip.landing_format,
            ip.watermark_data_type AS policy_watermark_data_type,
            st.watermark_column AS state_watermark_column,
            st.watermark_data_type AS state_watermark_data_type,
            st.state_version AS current_state_version
        FROM `{catalog}`.`{CONTROL_SCHEMA}`.`{RUN_OBJECT_TABLE}` AS ro
        INNER JOIN `{catalog}`.`{CONTROL_SCHEMA}`.`{RUN_TABLE}` AS r
            ON r.run_id = ro.run_id
        INNER JOIN `{catalog}`.`{CONTROL_SCHEMA}`.`source_object` AS so
            ON so.source_object_key = ro.source_object_key
        INNER JOIN `{catalog}`.`{CONTROL_SCHEMA}`.`ingestion_policy` AS ip
            ON ip.source_object_key = ro.source_object_key
        INNER JOIN `{catalog}`.`{CONTROL_SCHEMA}`.`{STATE_TABLE}` AS st
            ON st.source_object_key = ro.source_object_key
        WHERE ro.run_object_id = '{escape_sql_literal(run_object_id)}'
        """
    ).collect()

    if len(rows) != 1:
        raise RuntimeError(
            "Esperado exatamente um ingestion_run_object. "
            f"run_object_id={run_object_id}, encontrado={len(rows)}"
        )
    return rows[0]


def validate_context(row):
    if row["run_status"] not in {"CREATED", "RUNNING"}:
        raise RuntimeError(
            "O ingestion_run pai não aceita materialização. "
            f"run_status={row['run_status']}"
        )
    if row["status"] != "PENDING":
        raise RuntimeError(
            "A tentativa deve estar PENDING. "
            f"status={row['status']}"
        )
    if row["state_committed"]:
        raise RuntimeError("A tentativa já possui state_committed=true.")
    if row["state_version_before"] != row["current_state_version"]:
        raise RuntimeError(
            "Plano obsoleto: ingestion_state mudou desde o planejamento. "
            f"planejado={row['state_version_before']}, "
            f"atual={row['current_state_version']}"
        )

    landing_format = row["landing_format"].strip().upper()
    if landing_format not in SUPPORTED_LANDING_FORMATS:
        raise RuntimeError(f"Formato de Landing não suportado: {landing_format}")

    load_mode = row["load_mode"].strip().upper()
    if load_mode not in {"FULL", "INCREMENTAL"}:
        raise RuntimeError(f"load_mode inválido: {load_mode}")
    if int(row["attempt_number"]) < 1:
        raise RuntimeError("attempt_number deve ser >= 1.")

    watermark_column = row["watermark_column"]
    if watermark_column is None:
        if load_mode != "FULL":
            raise RuntimeError("INCREMENTAL sem watermark_column.")
        if (
            row["watermark_lower_bound"] is not None
            or row["watermark_upper_bound"] is not None
        ):
            raise RuntimeError("FULL permanente possui bounds indevidos.")
        return

    validate_identifier(watermark_column)
    policy_type = row["policy_watermark_data_type"]
    state_type = row["state_watermark_data_type"]
    state_column = row["state_watermark_column"]

    if policy_type is None:
        raise RuntimeError("watermark_data_type ausente na ingestion_policy.")
    if state_column != watermark_column or state_type != policy_type:
        raise RuntimeError(
            "Contrato de watermark divergente entre plano/policy/state. "
            f"plan_column={watermark_column}, state_column={state_column}, "
            f"policy_type={policy_type}, state_type={state_type}"
        )
    if load_mode == "INCREMENTAL" and row["watermark_lower_bound"] is None:
        raise RuntimeError("INCREMENTAL sem watermark_lower_bound.")


def build_landing_paths(row):
    landing_catalog = validate_identifier(row["landing_catalog"])
    landing_schema = validate_identifier(row["landing_schema"])
    landing_volume = validate_identifier(row["landing_volume"])
    relative_path = validate_relative_path(row["landing_relative_path"])
    run_id = validate_uuid(row["run_id"])
    extraction_date = row["run_started_at"].date().isoformat()
    attempt_number = int(row["attempt_number"])

    base_path = (
        f"/Volumes/{landing_catalog}/{landing_schema}/{landing_volume}/"
        f"{relative_path}/extraction_date={extraction_date}/"
        f"run_id={run_id}/attempt_number={attempt_number}"
    )
    return {
        "extraction_date": extraction_date,
        "base_path": base_path,
        "data_path": f"{base_path}/data",
        "manifest_path": f"{base_path}/_manifest",
    }


def build_watermark_literal(value: str, data_type: str):
    normalized_type = data_type.strip().upper()
    if normalized_type == "TIMESTAMP":
        return F.lit(value).cast("timestamp_ntz")
    if normalized_type == "DATE":
        return F.lit(value).cast("date")
    raise RuntimeError(f"watermark_data_type não suportado: {data_type}")


def build_source_dataframe(spark: SparkSession, row):
    source_fqn = validate_source_fqn(row["source_fqn"])
    source_df = spark.table(source_fqn)
    load_mode = row["load_mode"].strip().upper()
    watermark_column = row["watermark_column"]

    if load_mode == "FULL" and watermark_column is None:
        return source_df

    watermark_data_type = row["policy_watermark_data_type"]
    upper_text = row["watermark_upper_bound"]

    # MAX(cursor) pode ser NULL em uma origem vazia. O plano congelado
    # representa então um snapshot vazio e não deve capturar linhas futuras.
    if upper_text is None:
        return source_df.where(F.lit(False))

    upper_bound = build_watermark_literal(upper_text, watermark_data_type)
    if load_mode == "FULL":
        return source_df.where(F.col(watermark_column) <= upper_bound)

    lower_bound = build_watermark_literal(
        row["watermark_lower_bound"],
        watermark_data_type,
    )
    return source_df.where(
        (F.col(watermark_column) > lower_bound)
        & (F.col(watermark_column) <= upper_bound)
    )


def mark_parent_run_running(
    spark: SparkSession,
    catalog: str,
    run_id: str,
):
    spark.sql(
        f"""
        UPDATE `{catalog}`.`{CONTROL_SCHEMA}`.`{RUN_TABLE}`
        SET status = 'RUNNING', updated_at = current_timestamp()
        WHERE run_id = '{escape_sql_literal(run_id)}'
          AND status = 'CREATED'
        """
    )


def mark_running(
    spark: SparkSession,
    catalog: str,
    run_object_id: str,
):
    spark.sql(
        f"""
        UPDATE `{catalog}`.`{CONTROL_SCHEMA}`.`{RUN_OBJECT_TABLE}`
        SET
            status = 'RUNNING',
            started_at = current_timestamp(),
            completed_at = NULL,
            error_class = NULL,
            error_message = NULL,
            updated_at = current_timestamp()
        WHERE run_object_id = '{escape_sql_literal(run_object_id)}'
          AND status = 'PENDING'
        """
    )


def assert_status(
    spark: SparkSession,
    catalog: str,
    run_object_id: str,
    expected_status: str,
):
    row = spark.sql(
        f"""
        SELECT status
        FROM `{catalog}`.`{CONTROL_SCHEMA}`.`{RUN_OBJECT_TABLE}`
        WHERE run_object_id = '{escape_sql_literal(run_object_id)}'
        """
    ).first()
    if row is None or row["status"] != expected_status:
        actual = None if row is None else row["status"]
        raise RuntimeError(
            "Transição de estado não confirmada. "
            f"esperado={expected_status}, encontrado={actual}"
        )


def mark_materialized(
    spark: SparkSession,
    catalog: str,
    run_object_id: str,
    source_row_count: int,
    landing_row_count: int,
    data_path: str,
    manifest_path: str,
):
    spark.sql(
        f"""
        UPDATE `{catalog}`.`{CONTROL_SCHEMA}`.`{RUN_OBJECT_TABLE}`
        SET
            status = 'MATERIALIZED',
            source_row_count = {int(source_row_count)},
            landing_row_count = {int(landing_row_count)},
            landing_data_path = '{escape_sql_literal(data_path)}',
            manifest_path = '{escape_sql_literal(manifest_path)}',
            completed_at = current_timestamp(),
            updated_at = current_timestamp()
        WHERE run_object_id = '{escape_sql_literal(run_object_id)}'
          AND status = 'RUNNING'
        """
    )


def mark_failed(
    spark: SparkSession,
    catalog: str,
    run_object_id: str,
    error: Exception,
):
    error_class = escape_sql_literal(error.__class__.__name__)
    error_message = escape_sql_literal(str(error)[:4000])
    spark.sql(
        f"""
        UPDATE `{catalog}`.`{CONTROL_SCHEMA}`.`{RUN_OBJECT_TABLE}`
        SET
            status = 'FAILED',
            error_class = '{error_class}',
            error_message = '{error_message}',
            completed_at = current_timestamp(),
            updated_at = current_timestamp()
        WHERE run_object_id = '{escape_sql_literal(run_object_id)}'
        """
    )


def write_manifest(
    spark: SparkSession,
    row,
    paths,
    source_row_count: int,
    landing_row_count: int,
    source_schema_json: str,
):
    schema = StructType(
        [
            StructField("manifest_version", StringType(), False),
            StructField("run_id", StringType(), False),
            StructField("run_object_id", StringType(), False),
            StructField("source_object_key", StringType(), False),
            StructField("attempt_number", IntegerType(), False),
            StructField("source_fqn", StringType(), False),
            StructField("load_mode", StringType(), False),
            StructField("landing_format", StringType(), False),
            StructField("extraction_date", StringType(), False),
            StructField("watermark_column", StringType(), True),
            StructField("watermark_data_type", StringType(), True),
            StructField("watermark_lower_bound", StringType(), True),
            StructField("watermark_upper_bound", StringType(), True),
            StructField("lookback_days_applied", IntegerType(), True),
            StructField("state_version_before", LongType(), False),
            StructField("source_row_count", LongType(), False),
            StructField("landing_row_count", LongType(), False),
            StructField("source_schema_json", StringType(), False),
            StructField("landing_data_path", StringType(), False),
            StructField("materialized_at", TimestampType(), False),
        ]
    )
    values = [
        (
            "2.1",
            row["run_id"],
            row["run_object_id"],
            row["source_object_key"],
            int(row["attempt_number"]),
            row["source_fqn"],
            row["load_mode"],
            row["landing_format"],
            paths["extraction_date"],
            row["watermark_column"],
            row["policy_watermark_data_type"],
            row["watermark_lower_bound"],
            row["watermark_upper_bound"],
            row["lookback_days_applied"],
            int(row["state_version_before"]),
            int(source_row_count),
            int(landing_row_count),
            source_schema_json,
            paths["data_path"],
            utc_now(),
        )
    ]
    (
        spark.createDataFrame(values, schema=schema)
        .write.format("json")
        .mode("error")
        .save(paths["manifest_path"])
    )


def materialize(
    spark: SparkSession,
    catalog: str,
    row,
):
    run_object_id = row["run_object_id"]
    paths = build_landing_paths(row)

    print("=" * 60)
    print("MATERIALIZAÇÃO DE UM OBJETO")
    print(f"source_table......: {row['source_table']}")
    print(f"run_id............: {row['run_id']}")
    print(f"run_object_id.....: {run_object_id}")
    print(f"attempt_number....: {row['attempt_number']}")
    print(f"load_mode.........: {row['load_mode']}")
    print(f"lower_bound.......: {row['watermark_lower_bound']}")
    print(f"upper_bound.......: {row['watermark_upper_bound']}")
    print(f"data_path.........: {paths['data_path']}")
    print(f"manifest_path.....: {paths['manifest_path']}")
    print("=" * 60)

    mark_parent_run_running(spark, catalog, row["run_id"])
    mark_running(spark, catalog, run_object_id)
    assert_status(spark, catalog, run_object_id, "RUNNING")

    try:
        source_df = build_source_dataframe(spark, row)
        source_schema_json = source_df.schema.json()

        observation = Observation(f"materialize_{uuid.uuid4().hex}")
        observed_df = source_df.observe(
            observation,
            F.count(F.lit(1)).alias("source_row_count"),
        )

        landing_format = row["landing_format"].strip().lower()
        (
            observed_df.write.format(landing_format)
            .mode("error")
            .save(paths["data_path"])
        )
        source_row_count = int(observation.get["source_row_count"])

        # Schema explícito mantém o read-back válido mesmo para lote vazio.
        landing_df = (
            spark.read.schema(source_df.schema)
            .format(landing_format)
            .load(paths["data_path"])
        )
        landing_row_count = int(landing_df.count())

        print(f"source_row_count..: {source_row_count}")
        print(f"landing_row_count.: {landing_row_count}")

        if source_row_count != landing_row_count:
            raise RuntimeError(
                "Divergência source/landing. "
                f"source={source_row_count}, landing={landing_row_count}"
            )

        write_manifest(
            spark,
            row,
            paths,
            source_row_count,
            landing_row_count,
            source_schema_json,
        )
        mark_materialized(
            spark,
            catalog,
            run_object_id,
            source_row_count,
            landing_row_count,
            paths["data_path"],
            paths["manifest_path"],
        )
        assert_status(spark, catalog, run_object_id, "MATERIALIZED")

        print("=" * 60)
        print("MATERIALIZAÇÃO CONCLUÍDA COM SUCESSO")
        print("ingestion_state não foi alterado.")
        print("=" * 60)
    except Exception as exc:
        mark_failed(spark, catalog, run_object_id, exc)
        raise


def main():
    args = parse_args()
    catalog = validate_identifier(args.catalog)
    run_object_id = validate_uuid(args.run_object_id)
    spark = SparkSession.builder.getOrCreate()

    row = get_run_object_context(spark, catalog, run_object_id)
    validate_context(row)
    materialize(spark, catalog, row)


if __name__ == "__main__":
    main()

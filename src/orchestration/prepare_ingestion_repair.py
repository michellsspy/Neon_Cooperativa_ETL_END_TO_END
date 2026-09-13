# Databricks notebook source

import re
import uuid

from datetime import datetime
from datetime import timezone

from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


# ============================================================
# CONSTANTES
# ============================================================

CONTROL_SCHEMA = "control"

RUN_TABLE = "ingestion_run"
RUN_OBJECT_TABLE = "ingestion_run_object"
STATE_TABLE = "ingestion_state"


# ============================================================
# WIDGETS
# ============================================================

dbutils.widgets.text(
    "catalog",
    "",
)

dbutils.widgets.text(
    "run_id",
    "",
)


catalog = (
    dbutils.widgets
    .get("catalog")
    .strip()
)

run_id = (
    dbutils.widgets
    .get("run_id")
    .strip()
)


# ============================================================
# UTILITÁRIOS
# ============================================================

def validate_identifier(value: str) -> str:

    if not re.fullmatch(
        r"[A-Za-z0-9_]+",
        value,
    ):
        raise ValueError(
            f"Identificador inválido: {value}"
        )

    return value


def validate_uuid(value: str) -> str:

    try:

        return str(
            uuid.UUID(value)
        )

    except ValueError as exc:

        raise ValueError(
            f"UUID inválido: {value}"
        ) from exc


def escape_sql_literal(value: str) -> str:

    return value.replace(
        "'",
        "''",
    )


def utc_now() -> datetime:

    return (
        datetime
        .now(timezone.utc)
        .replace(tzinfo=None)
    )


def generate_run_object_id(
    run_id: str,
    source_object_key: str,
    attempt_number: int,
) -> str:
    """
    Mesma regra determinística utilizada pelo planner.

    A identidade de uma tentativa é:

        run
        + objeto
        + attempt_number
    """

    deterministic_key = (
        f"{run_id}:"
        f"{source_object_key}:"
        f"attempt:{attempt_number}"
    )

    return str(
        uuid.uuid5(
            uuid.NAMESPACE_URL,
            deterministic_key,
        )
    )


catalog = validate_identifier(
    catalog
)

run_id = validate_uuid(
    run_id
)

run_literal = escape_sql_literal(
    run_id
)


# ============================================================
# RUN PAI
# ============================================================

run_rows = spark.sql(
    f"""
    SELECT
        run_id,
        status

    FROM
        `{catalog}`.`{CONTROL_SCHEMA}`.`{RUN_TABLE}`

    WHERE
        run_id = '{run_literal}'
    """
).collect()


if len(run_rows) != 1:

    raise RuntimeError(
        "Esperado exatamente um ingestion_run. "
        f"run_id={run_id}, "
        f"encontrado={len(run_rows)}"
    )


run_status = (
    run_rows[0]["status"]
)


if run_status not in {
    "CREATED",
    "RUNNING",
    "FAILED",
}:

    raise RuntimeError(
        "Run não está disponível para recuperação. "
        f"status={run_status}"
    )


# ============================================================
# ÚLTIMA TENTATIVA POR OBJETO
# ============================================================

def get_latest_attempts():

    return spark.sql(
        f"""
        WITH ranked_attempts AS
        (
            SELECT

                ro.*,

                ROW_NUMBER() OVER (
                    PARTITION BY
                        ro.source_object_key

                    ORDER BY
                        ro.attempt_number DESC,
                        ro.created_at DESC,
                        ro.run_object_id DESC
                ) AS rn

            FROM
                `{catalog}`.`{CONTROL_SCHEMA}`.`{RUN_OBJECT_TABLE}`
                AS ro

            WHERE
                ro.run_id =
                    '{run_literal}'
        )

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

            so.source_table,

            ip.retry_enabled,
            ip.max_retries,

            st.state_version
                AS current_state_version

        FROM
            ranked_attempts AS ro

        INNER JOIN
            `{catalog}`.`{CONTROL_SCHEMA}`.`source_object`
            AS so

            ON
                so.source_object_key
                =
                ro.source_object_key

        INNER JOIN
            `{catalog}`.`{CONTROL_SCHEMA}`.`ingestion_policy`
            AS ip

            ON
                ip.source_object_key
                =
                ro.source_object_key

        INNER JOIN
            `{catalog}`.`{CONTROL_SCHEMA}`.`{STATE_TABLE}`
            AS st

            ON
                st.source_object_key
                =
                ro.source_object_key

        WHERE
            ro.rn = 1

        ORDER BY
            so.source_table
        """
    ).collect()


latest_attempts = (
    get_latest_attempts()
)


if not latest_attempts:

    raise RuntimeError(
        "Nenhum run_object encontrado "
        f"para run_id={run_id}"
    )


# ============================================================
# VALIDAÇÃO PRÉVIA
# ============================================================

errors = []


for row in latest_attempts:

    source_table = (
        row["source_table"]
    )

    status = (
        row["status"]
    )

    if row["state_committed"]:

        errors.append(
            f"{source_table}: "
            "state_committed=true"
        )

        continue

    if (
        int(
            row["state_version_before"]
        )
        !=
        int(
            row["current_state_version"]
        )
    ):

        errors.append(
            f"{source_table}: "
            "state_version mudou desde o plano "
            f"({row['state_version_before']} -> "
            f"{row['current_state_version']})"
        )

        continue

    if status == "RUNNING":

        errors.append(
            f"{source_table}: "
            "tentativa permanece RUNNING; "
            "resultado físico é ambíguo"
        )

    elif status in {
        "RECONCILED",
        "SUCCEEDED",
    }:

        errors.append(
            f"{source_table}: "
            f"status={status}; recuperação está "
            "além da fase de materialização"
        )

    elif status not in {
        "PENDING",
        "MATERIALIZED",
        "FAILED",
        "SKIPPED",
    }:

        errors.append(
            f"{source_table}: "
            f"status inesperado={status}"
        )


if errors:

    raise RuntimeError(
        "Run não pode ser recuperado automaticamente: "
        + "; ".join(errors)
    )


# ============================================================
# CONSTRUÇÃO DAS NOVAS TENTATIVAS
# ============================================================

now = utc_now()

retry_rows = []


for row in latest_attempts:

    if row["status"] not in {
        "FAILED",
        "SKIPPED",
    }:

        continue

    source_table = (
        row["source_table"]
    )

    if not row["retry_enabled"]:

        raise RuntimeError(
            f"{source_table}: retry desabilitado "
            "pela ingestion_policy."
        )

    current_attempt = int(
        row["attempt_number"]
    )

    retries_already_used = (
        current_attempt - 1
    )

    max_retries = int(
        row["max_retries"]
    )

    if (
        retries_already_used
        >=
        max_retries
    ):

        raise RuntimeError(
            f"{source_table}: limite de retries "
            "atingido. "
            f"attempt={current_attempt}, "
            f"max_retries={max_retries}"
        )

    next_attempt = (
        current_attempt + 1
    )

    next_run_object_id = (
        generate_run_object_id(
            run_id=run_id,
            source_object_key=(
                row[
                    "source_object_key"
                ]
            ),
            attempt_number=(
                next_attempt
            ),
        )
    )

    retry_rows.append(
        (
            next_run_object_id,

            row["run_id"],

            row[
                "source_object_key"
            ],

            next_attempt,

            row[
                "run_object_id"
            ],

            row[
                "load_mode"
            ],

            "PENDING",

            row[
                "watermark_column"
            ],

            row[
                "watermark_lower_bound"
            ],

            row[
                "watermark_upper_bound"
            ],

            row[
                "lookback_days_applied"
            ],

            None,
            None,

            None,
            None,

            int(
                row[
                    "state_version_before"
                ]
            ),

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


# ============================================================
# PERSISTÊNCIA DAS NOVAS TENTATIVAS
# ============================================================

if retry_rows:

    retry_schema = StructType(
        [
            StructField(
                "run_object_id",
                StringType(),
                False,
            ),

            StructField(
                "run_id",
                StringType(),
                False,
            ),

            StructField(
                "source_object_key",
                StringType(),
                False,
            ),

            StructField(
                "attempt_number",
                IntegerType(),
                False,
            ),

            StructField(
                "reprocess_of_run_object_id",
                StringType(),
                True,
            ),

            StructField(
                "load_mode",
                StringType(),
                False,
            ),

            StructField(
                "status",
                StringType(),
                False,
            ),

            StructField(
                "watermark_column",
                StringType(),
                True,
            ),

            StructField(
                "watermark_lower_bound",
                StringType(),
                True,
            ),

            StructField(
                "watermark_upper_bound",
                StringType(),
                True,
            ),

            StructField(
                "lookback_days_applied",
                IntegerType(),
                True,
            ),

            StructField(
                "source_row_count",
                LongType(),
                True,
            ),

            StructField(
                "landing_row_count",
                LongType(),
                True,
            ),

            StructField(
                "landing_data_path",
                StringType(),
                True,
            ),

            StructField(
                "manifest_path",
                StringType(),
                True,
            ),

            StructField(
                "state_version_before",
                LongType(),
                False,
            ),

            StructField(
                "state_version_after",
                LongType(),
                True,
            ),

            StructField(
                "state_committed",
                BooleanType(),
                False,
            ),

            StructField(
                "error_class",
                StringType(),
                True,
            ),

            StructField(
                "error_message",
                StringType(),
                True,
            ),

            StructField(
                "started_at",
                TimestampType(),
                True,
            ),

            StructField(
                "completed_at",
                TimestampType(),
                True,
            ),

            StructField(
                "created_at",
                TimestampType(),
                False,
            ),

            StructField(
                "updated_at",
                TimestampType(),
                False,
            ),
        ]
    )

    retry_df = spark.createDataFrame(
        retry_rows,
        schema=retry_schema,
    )

    retry_df.createOrReplaceTempView(
        "repair_ingestion_attempts"
    )

    spark.sql(
        f"""
        MERGE INTO
            `{catalog}`.`{CONTROL_SCHEMA}`.`{RUN_OBJECT_TABLE}`
            AS tgt

        USING
            repair_ingestion_attempts
            AS src

        ON
            tgt.run_object_id
            =
            src.run_object_id

        WHEN NOT MATCHED THEN

            INSERT
            (
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
            )

            VALUES
            (
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


# ============================================================
# REABRE O RUN PARA RECUPERAÇÃO
# ============================================================

spark.sql(
    f"""
    UPDATE
        `{catalog}`.`{CONTROL_SCHEMA}`.`{RUN_TABLE}`

    SET
        status =
            'RUNNING',

        completed_at =
            NULL,

        updated_at =
            current_timestamp()

    WHERE
        run_id =
            '{run_literal}'

        AND status IN (
            'CREATED',
            'RUNNING',
            'FAILED'
        )
    """
)


# ============================================================
# RECARREGA AS TENTATIVAS ATUAIS
# ============================================================

latest_attempts = (
    get_latest_attempts()
)


# ============================================================
# GATE FINAL
# ============================================================

invalid_after_prepare = [
    row
    for row in latest_attempts
    if row["status"] not in {
        "PENDING",
        "MATERIALIZED",
    }
]


if invalid_after_prepare:

    details = "; ".join(
        (
            f"{row['source_table']}="
            f"{row['status']}"
        )
        for row in invalid_after_prepare
    )

    raise RuntimeError(
        "Estado inesperado após preparação do repair: "
        f"{details}"
    )


# ============================================================
# SOMENTE OBJETOS QUE PRECISAM MATERIALIZAÇÃO
# ============================================================

run_objects = [
    {
        "run_object_id": (
            row[
                "run_object_id"
            ]
        ),

        "source_table": (
            row[
                "source_table"
            ]
        ),

        "load_mode": (
            row[
                "load_mode"
            ]
        ),

        "attempt_number": int(
            row[
                "attempt_number"
            ]
        ),
    }

    for row in latest_attempts

    if row["status"] == "PENDING"
]


if not run_objects:

    raise RuntimeError(
        "Nenhum objeto PENDING foi encontrado para "
        "recuperação. Este run não requer repair "
        "da fase de materialização."
    )


# ============================================================
# TASK VALUES
# ============================================================

dbutils.jobs.taskValues.set(
    key="run_id",
    value=run_id,
)

dbutils.jobs.taskValues.set(
    key="run_objects",
    value=run_objects,
)

dbutils.jobs.taskValues.set(
    key="materialize_count",
    value=len(
        run_objects
    ),
)


# ============================================================
# LOG
# ============================================================

print(
    "============================================================"
)

print(
    "REPAIR DE INGESTÃO PREPARADO"
)

print(
    "============================================================"
)

print(
    f"run_id................: "
    f"{run_id}"
)

print(
    f"novas tentativas......: "
    f"{len(retry_rows)}"
)

print(
    f"objetos a materializar: "
    f"{len(run_objects)}"
)

print(
    "------------------------------------------------------------"
)


for item in run_objects:

    print(
        f"{item['source_table']}: "
        f"attempt={item['attempt_number']} | "
        f"{item['load_mode']} | "
        f"run_object_id="
        f"{item['run_object_id']}"
    )


print(
    "============================================================"
)
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
    "source_run_object_id",
    "",
)

dbutils.widgets.text(
    "requested_by",
    "",
)

dbutils.widgets.text(
    "orchestrator_job_id",
    "",
)

dbutils.widgets.text(
    "orchestrator_run_id",
    "",
)


catalog = (
    dbutils.widgets
    .get("catalog")
    .strip()
)

source_run_object_id = (
    dbutils.widgets
    .get("source_run_object_id")
    .strip()
)

requested_by = (
    dbutils.widgets
    .get("requested_by")
    .strip()
)

orchestrator_job_id = (
    dbutils.widgets
    .get("orchestrator_job_id")
    .strip()
)

orchestrator_run_id = (
    dbutils.widgets
    .get("orchestrator_run_id")
    .strip()
)


# ============================================================
# UTILITÁRIOS
# ============================================================

def validate_identifier(
    value: str,
) -> str:

    if not re.fullmatch(
        r"[A-Za-z0-9_]+",
        value,
    ):
        raise ValueError(
            f"Identificador inválido: {value}"
        )

    return value


def validate_uuid(
    value: str,
) -> str:

    try:

        return str(
            uuid.UUID(value)
        )

    except ValueError as exc:

        raise ValueError(
            f"UUID inválido: {value}"
        ) from exc


def validate_reference(
    value: str,
    field_name: str,
) -> str:

    if not value:

        raise ValueError(
            f"{field_name} não informado."
        )

    if not re.fullmatch(
        r"[A-Za-z0-9_.@+-]+",
        value,
    ):

        raise ValueError(
            f"{field_name} inválido: {value}"
        )

    return value


def escape_sql_literal(
    value: str,
) -> str:

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


def generate_run_id(
    job_id: str,
    job_run_id: str,
) -> str:
    """
    O reprocessamento é uma nova execução lógica.

    Repair do próprio Lakeflow Job reutiliza o mesmo
    run lógico porque job.run_id permanece a identidade
    da execução.
    """

    key = (
        "reprocess:"
        f"{job_id}:"
        f"{job_run_id}"
    )

    return str(
        uuid.uuid5(
            uuid.NAMESPACE_URL,
            key,
        )
    )


def generate_run_object_id(
    run_id: str,
    source_object_key: str,
) -> str:

    key = (
        f"{run_id}:"
        f"{source_object_key}:"
        "attempt:1"
    )

    return str(
        uuid.uuid5(
            uuid.NAMESPACE_URL,
            key,
        )
    )


# ============================================================
# VALIDAÇÃO DOS PARÂMETROS
# ============================================================

catalog = validate_identifier(
    catalog
)

source_run_object_id = validate_uuid(
    source_run_object_id
)

requested_by = validate_reference(
    requested_by,
    "requested_by",
)

orchestrator_job_id = validate_reference(
    orchestrator_job_id,
    "orchestrator_job_id",
)

orchestrator_run_id = validate_reference(
    orchestrator_run_id,
    "orchestrator_run_id",
)


source_run_object_literal = (
    escape_sql_literal(
        source_run_object_id
    )
)


# ============================================================
# OBJETO HISTÓRICO QUE SERÁ REPROCESSADO
# ============================================================

source_rows = spark.sql(
    f"""
    SELECT

        ro.run_object_id,
        ro.run_id,
        ro.source_object_key,

        ro.attempt_number,
        ro.load_mode,
        ro.status,

        ro.watermark_column,
        ro.watermark_lower_bound,
        ro.watermark_upper_bound,

        ro.lookback_days_applied,

        ro.source_row_count,
        ro.landing_row_count,

        ro.state_committed,

        so.source_table,

        ip.manual_reprocessing_enabled,

        st.state_version
            AS current_state_version,

        r.status
            AS source_run_status,

        r.schedule_group
            AS source_schedule_group

    FROM
        `{catalog}`.`{CONTROL_SCHEMA}`.`{RUN_OBJECT_TABLE}`
        AS ro

    INNER JOIN
        `{catalog}`.`{CONTROL_SCHEMA}`.`{RUN_TABLE}`
        AS r

        ON
            r.run_id =
            ro.run_id

    INNER JOIN
        `{catalog}`.`{CONTROL_SCHEMA}`.`source_object`
        AS so

        ON
            so.source_object_key =
            ro.source_object_key

    INNER JOIN
        `{catalog}`.`{CONTROL_SCHEMA}`.`ingestion_policy`
        AS ip

        ON
            ip.source_object_key =
            ro.source_object_key

    INNER JOIN
        `{catalog}`.`{CONTROL_SCHEMA}`.`{STATE_TABLE}`
        AS st

        ON
            st.source_object_key =
            ro.source_object_key

    WHERE
        ro.run_object_id =
            '{source_run_object_literal}'
    """
).collect()


if len(source_rows) != 1:

    raise RuntimeError(
        "Esperado exatamente um run_object histórico. "
        f"Encontrado={len(source_rows)}"
    )


source = source_rows[0]


# ============================================================
# GATES DO REPROCESSAMENTO
# ============================================================

if (
    source["source_run_status"]
    != "SUCCEEDED"
):

    raise RuntimeError(
        "O run histórico precisa estar SUCCEEDED. "
        f"Encontrado={source['source_run_status']}"
    )


if (
    source["status"]
    != "SUCCEEDED"
):

    raise RuntimeError(
        "A tentativa histórica precisa estar SUCCEEDED. "
        f"Encontrado={source['status']}"
    )


if not source[
    "manual_reprocessing_enabled"
]:

    raise RuntimeError(
        "Reprocessamento manual está desabilitado "
        f"para {source['source_table']}."
    )


if (
    source["source_row_count"]
    is None
    or
    source["landing_row_count"]
    is None
):

    raise RuntimeError(
        "Execução histórica não possui "
        "contagens reconciliáveis."
    )


if (
    source["source_row_count"]
    !=
    source["landing_row_count"]
):

    raise RuntimeError(
        "Execução histórica possui divergência "
        "source x Landing."
    )


# ============================================================
# IDENTIDADES DA NOVA EXECUÇÃO
# ============================================================

run_id = generate_run_id(
    job_id=orchestrator_job_id,
    job_run_id=orchestrator_run_id,
)

run_object_id = (
    generate_run_object_id(
        run_id=run_id,
        source_object_key=(
            source[
                "source_object_key"
            ]
        ),
    )
)


now = utc_now()


# ============================================================
# NOVO INGESTION_RUN
# ============================================================

run_schema = StructType(
    [
        StructField(
            "run_id",
            StringType(),
            False,
        ),

        StructField(
            "trigger_type",
            StringType(),
            False,
        ),

        StructField(
            "trigger_reference",
            StringType(),
            True,
        ),

        StructField(
            "schedule_group",
            StringType(),
            True,
        ),

        StructField(
            "requested_by",
            StringType(),
            True,
        ),

        StructField(
            "orchestrator_job_id",
            StringType(),
            True,
        ),

        StructField(
            "orchestrator_run_id",
            StringType(),
            True,
        ),

        StructField(
            "status",
            StringType(),
            False,
        ),

        StructField(
            "started_at",
            TimestampType(),
            False,
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


run_df = spark.createDataFrame(
    [
        (
            run_id,

            "REPROCESS",

            (
                "run_object:"
                f"{source_run_object_id}"
            ),

            source[
                "source_schedule_group"
            ],

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
    schema=run_schema,
)


run_df.createOrReplaceTempView(
    "manual_reprocess_run"
)


spark.sql(
    f"""
    MERGE INTO
        `{catalog}`.`{CONTROL_SCHEMA}`.`{RUN_TABLE}`
        AS tgt

    USING
        manual_reprocess_run
        AS src

    ON
        tgt.run_id =
        src.run_id

    WHEN NOT MATCHED THEN

        INSERT
        (
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
        )

        VALUES
        (
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


# ============================================================
# NOVO RUN_OBJECT
#
# A janela histórica é preservada.
#
# state_version_before usa a versão ATUAL porque o
# materializador deve garantir que nenhum commit concorrente
# ocorreu entre a preparação e a materialização.
# ============================================================

run_object_schema = StructType(
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


run_object_df = spark.createDataFrame(
    [
        (
            run_object_id,

            run_id,

            source[
                "source_object_key"
            ],

            1,

            source_run_object_id,

            source[
                "load_mode"
            ],

            "PENDING",

            source[
                "watermark_column"
            ],

            source[
                "watermark_lower_bound"
            ],

            source[
                "watermark_upper_bound"
            ],

            source[
                "lookback_days_applied"
            ],

            None,
            None,

            None,
            None,

            int(
                source[
                    "current_state_version"
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
    ],
    schema=run_object_schema,
)


run_object_df.createOrReplaceTempView(
    "manual_reprocess_run_object"
)


spark.sql(
    f"""
    MERGE INTO
        `{catalog}`.`{CONTROL_SCHEMA}`.`{RUN_OBJECT_TABLE}`
        AS tgt

    USING
        manual_reprocess_run_object
        AS src

    ON
        tgt.run_object_id =
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
# TASK VALUES
# ============================================================

dbutils.jobs.taskValues.set(
    key="run_id",
    value=run_id,
)

dbutils.jobs.taskValues.set(
    key="run_object_id",
    value=run_object_id,
)

dbutils.jobs.taskValues.set(
    key="source_run_object_id",
    value=source_run_object_id,
)


# ============================================================
# LOG
# ============================================================

print(
    "============================================================"
)

print(
    "REPROCESSAMENTO MANUAL PREPARADO"
)

print(
    "============================================================"
)

print(
    f"source_table...............: "
    f"{source['source_table']}"
)

print(
    f"source_run_object_id.......: "
    f"{source_run_object_id}"
)

print(
    f"novo run_id................: "
    f"{run_id}"
)

print(
    f"novo run_object_id.........: "
    f"{run_object_id}"
)

print(
    f"load_mode..................: "
    f"{source['load_mode']}"
)

print(
    f"watermark_lower_bound......: "
    f"{source['watermark_lower_bound']}"
)

print(
    f"watermark_upper_bound......: "
    f"{source['watermark_upper_bound']}"
)

print(
    f"state_version_before.......: "
    f"{source['current_state_version']}"
)

print(
    "============================================================"
)
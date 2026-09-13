# Databricks notebook source

import re


# ============================================================
# WIDGETS
# ============================================================

dbutils.widgets.text(
    "catalog",
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


# ============================================================
# ARGUMENTOS
# ============================================================

catalog = (
    dbutils.widgets
    .get("catalog")
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
# VALIDAÇÕES
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


def validate_job_reference(
    value: str,
    field_name: str,
) -> str:

    if not value:

        raise ValueError(
            f"{field_name} não informado."
        )

    if not re.fullmatch(
        r"[A-Za-z0-9_-]+",
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


catalog = validate_identifier(
    catalog
)

orchestrator_job_id = (
    validate_job_reference(
        orchestrator_job_id,
        "orchestrator_job_id",
    )
)

orchestrator_run_id = (
    validate_job_reference(
        orchestrator_run_id,
        "orchestrator_run_id",
    )
)


CONTROL_SCHEMA = "control"


# ============================================================
# LOCALIZA O RUN GERADO PELO PLANNER
# ============================================================

job_id_literal = escape_sql_literal(
    orchestrator_job_id
)

job_run_id_literal = (
    escape_sql_literal(
        orchestrator_run_id
    )
)


run_rows = spark.sql(
    f"""
    SELECT
        run_id,
        status,
        created_at

    FROM
        `{catalog}`.`{CONTROL_SCHEMA}`.`ingestion_run`

    WHERE
        orchestrator_job_id =
            '{job_id_literal}'

        AND orchestrator_run_id =
            '{job_run_id_literal}'

    ORDER BY
        created_at DESC
    """
).collect()


if len(run_rows) != 1:

    raise RuntimeError(
        "Esperado exatamente um ingestion_run "
        "para a execução atual do Lakeflow Job. "
        f"Encontrado={len(run_rows)}"
    )


run = run_rows[0]

run_id = run[
    "run_id"
]

run_status = run[
    "status"
]


if run_status not in {
    "CREATED",
    "RUNNING",
}:

    raise RuntimeError(
        "O ingestion_run encontrado não está "
        "disponível para materialização. "
        f"run_id={run_id}, "
        f"status={run_status}"
    )


# ============================================================
# TENTATIVAS ATUAIS
# ============================================================

run_id_literal = escape_sql_literal(
    run_id
)


object_rows = spark.sql(
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
            `{catalog}`.`{CONTROL_SCHEMA}`.`ingestion_run_object`
            AS ro

        WHERE
            ro.run_id =
                '{run_id_literal}'
    )

    SELECT

        ro.run_object_id,

        ro.source_object_key,

        ro.attempt_number,

        ro.load_mode,

        ro.status,

        so.source_table

    FROM
        ranked_attempts AS ro

    INNER JOIN
        `{catalog}`.`{CONTROL_SCHEMA}`.`source_object`
        AS so

        ON
            so.source_object_key
            =
            ro.source_object_key

    WHERE
        ro.rn = 1

    ORDER BY
        so.source_table
    """
).collect()


if not object_rows:

    raise RuntimeError(
        f"Nenhum run_object encontrado "
        f"para run_id={run_id}"
    )


# ============================================================
# GATE DE EXECUÇÃO LIMPA
# ============================================================

invalid_rows = [
    row
    for row in object_rows
    if row["status"] != "PENDING"
]


if invalid_rows:

    details = "; ".join(
        (
            f"{row['source_table']}="
            f"{row['status']}"
        )
        for row in invalid_rows
    )

    raise RuntimeError(
        "O adapter de execução automática exige "
        "um plano novo com todas as tentativas PENDING. "
        f"Encontrado: {details}"
    )


# ============================================================
# INPUT DO FOR EACH
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

    for row in object_rows
]


# ============================================================
# PUBLICAÇÃO DOS TASK VALUES
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
    key="object_count",
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
    "PLANO PUBLICADO PARA ORQUESTRAÇÃO"
)

print(
    "============================================================"
)

print(
    f"run_id..........: {run_id}"
)

print(
    f"objetos.........: {len(run_objects)}"
)

print(
    "------------------------------------------------------------"
)


for item in run_objects:

    print(
        f"{item['source_table']}: "
        f"{item['load_mode']} | "
        f"run_object_id="
        f"{item['run_object_id']}"
    )


print(
    "============================================================"
)
import argparse
import re
import uuid

from pyspark.sql import SparkSession


# ============================================================
# CONSTANTES
# ============================================================

CONTROL_SCHEMA = "control"

RUN_TABLE = "ingestion_run"
RUN_OBJECT_TABLE = "ingestion_run_object"
STATE_TABLE = "ingestion_state"


# ============================================================
# ARGUMENTOS
# ============================================================

def parse_args():

    parser = argparse.ArgumentParser(
        description=(
            "Finaliza um reprocessamento reconciliado "
            "sem modificar ingestion_state."
        )
    )

    parser.add_argument(
        "--catalog",
        required=True,
    )

    parser.add_argument(
        "--run-id",
        required=True,
    )

    return parser.parse_args()


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


# ============================================================
# MAIN
# ============================================================

def main():

    args = parse_args()

    catalog = validate_identifier(
        args.catalog
    )

    run_id = validate_uuid(
        args.run_id
    )

    run_literal = escape_sql_literal(
        run_id
    )

    spark = (
        SparkSession
        .builder
        .getOrCreate()
    )

    # --------------------------------------------------------
    # Run
    # --------------------------------------------------------

    run_rows = spark.sql(
        f"""
        SELECT
            run_id,
            trigger_type,
            status

        FROM
            `{catalog}`.`{CONTROL_SCHEMA}`.`{RUN_TABLE}`

        WHERE
            run_id =
                '{run_literal}'
        """
    ).collect()

    if len(run_rows) != 1:

        raise RuntimeError(
            "Esperado exatamente um ingestion_run."
        )

    run = run_rows[0]

    if (
        run["trigger_type"]
        != "REPROCESS"
    ):

        raise RuntimeError(
            "Este finalizador aceita somente "
            "trigger_type=REPROCESS."
        )

    if (
        run["status"]
        != "RUNNING"
    ):

        raise RuntimeError(
            "Reprocessamento precisa estar RUNNING. "
            f"status={run['status']}"
        )

    # --------------------------------------------------------
    # Última tentativa por objeto
    # --------------------------------------------------------

    rows = spark.sql(
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
            ro.source_object_key,

            ro.status,

            ro.state_version_before,
            ro.state_version_after,
            ro.state_committed,

            st.state_version
                AS current_state_version,

            st.last_successful_run_id,

            so.source_table

        FROM
            ranked_attempts AS ro

        INNER JOIN
            `{catalog}`.`{CONTROL_SCHEMA}`.`{STATE_TABLE}`
            AS st

            ON
                st.source_object_key
                =
                ro.source_object_key

        INNER JOIN
            `{catalog}`.`{CONTROL_SCHEMA}`.`source_object`
            AS so

            ON
                so.source_object_key
                =
                ro.source_object_key

        WHERE
            ro.rn = 1
        """
    ).collect()

    if len(rows) != 1:

        raise RuntimeError(
            "O reprocessamento manual atual deve "
            "possuir exatamente um objeto."
        )

    row = rows[0]

    # --------------------------------------------------------
    # Gate principal
    # --------------------------------------------------------

    if (
        row["status"]
        != "RECONCILED"
    ):

        raise RuntimeError(
            "Objeto ainda não está RECONCILED. "
            f"status={row['status']}"
        )

    if row["state_committed"]:

        raise RuntimeError(
            "Reprocessamento não pode possuir "
            "state_committed=true."
        )

    if (
        row["state_version_after"]
        is not None
    ):

        raise RuntimeError(
            "Reprocessamento não deve possuir "
            "state_version_after."
        )

    if (
        int(
            row["current_state_version"]
        )
        !=
        int(
            row["state_version_before"]
        )
    ):

        raise RuntimeError(
            "ingestion_state mudou durante o "
            "reprocessamento. "
            f"before={row['state_version_before']}, "
            f"current={row['current_state_version']}"
        )

    if (
        row["last_successful_run_id"]
        ==
        run_id
    ):

        raise RuntimeError(
            "Reprocessamento alterou indevidamente "
            "last_successful_run_id."
        )

    # --------------------------------------------------------
    # RECONCILED -> SUCCEEDED
    #
    # SUCCEEDED aqui significa que a operação terminou
    # corretamente.
    #
    # state_committed permanece FALSE, registrando
    # explicitamente que não houve avanço do checkpoint.
    # --------------------------------------------------------

    run_object_literal = (
        escape_sql_literal(
            row["run_object_id"]
        )
    )

    spark.sql(
        f"""
        UPDATE
            `{catalog}`.`{CONTROL_SCHEMA}`.`{RUN_OBJECT_TABLE}`

        SET
            status =
                'SUCCEEDED',

            completed_at =
                current_timestamp(),

            updated_at =
                current_timestamp()

        WHERE
            run_object_id =
                '{run_object_literal}'

            AND status =
                'RECONCILED'

            AND state_committed =
                FALSE
        """
    )

    # --------------------------------------------------------
    # Finaliza envelope
    # --------------------------------------------------------

    spark.sql(
        f"""
        UPDATE
            `{catalog}`.`{CONTROL_SCHEMA}`.`{RUN_TABLE}`

        SET
            status =
                'SUCCEEDED',

            completed_at =
                current_timestamp(),

            updated_at =
                current_timestamp()

        WHERE
            run_id =
                '{run_literal}'

            AND status =
                'RUNNING'
        """
    )

    # --------------------------------------------------------
    # Validação final
    # --------------------------------------------------------

    result = spark.sql(
        f"""
        SELECT

            r.status AS run_status,

            ro.status AS run_object_status,

            ro.state_committed,

            ro.state_version_before,
            ro.state_version_after,

            st.state_version
                AS current_state_version,

            st.last_successful_run_id

        FROM
            `{catalog}`.`{CONTROL_SCHEMA}`.`{RUN_TABLE}`
            AS r

        INNER JOIN
            `{catalog}`.`{CONTROL_SCHEMA}`.`{RUN_OBJECT_TABLE}`
            AS ro

            ON
                ro.run_id =
                r.run_id

        INNER JOIN
            `{catalog}`.`{CONTROL_SCHEMA}`.`{STATE_TABLE}`
            AS st

            ON
                st.source_object_key
                =
                ro.source_object_key

        WHERE
            r.run_id =
                '{run_literal}'
        """
    ).first()

    if (
        result["run_status"]
        != "SUCCEEDED"
    ):

        raise RuntimeError(
            "Run de reprocessamento não foi finalizado."
        )

    if (
        result["run_object_status"]
        != "SUCCEEDED"
    ):

        raise RuntimeError(
            "run_object de reprocessamento não "
            "foi finalizado."
        )

    if result["state_committed"]:

        raise RuntimeError(
            "Reprocessamento alterou state_committed."
        )

    if (
        result["state_version_after"]
        is not None
    ):

        raise RuntimeError(
            "Reprocessamento alterou "
            "state_version_after."
        )

    if (
        int(
            result["current_state_version"]
        )
        !=
        int(
            result["state_version_before"]
        )
    ):

        raise RuntimeError(
            "ingestion_state mudou indevidamente."
        )

    print(
        "============================================================"
    )

    print(
        "REPROCESSAMENTO FINALIZADO"
    )

    print(
        "============================================================"
    )

    print(
        f"source_table.........: "
        f"{row['source_table']}"
    )

    print(
        f"run_status...........: "
        f"{result['run_status']}"
    )

    print(
        f"run_object_status....: "
        f"{result['run_object_status']}"
    )

    print(
        f"state_committed......: "
        f"{result['state_committed']}"
    )

    print(
        f"state_version_before.: "
        f"{result['state_version_before']}"
    )

    print(
        f"state_version_after..: "
        f"{result['state_version_after']}"
    )

    print(
        f"current_state_version: "
        f"{result['current_state_version']}"
    )

    print(
        "============================================================"
    )

    print(
        "ingestion_state permaneceu inalterado."
    )


if __name__ == "__main__":
    main()
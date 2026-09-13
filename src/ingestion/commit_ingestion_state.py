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
            "Confirma no ingestion_state uma execução "
            "já materializada e reconciliada."
        )
    )

    parser.add_argument(
        "--catalog",
        required=True,
        help="Catálogo gerenciado do projeto.",
    )

    parser.add_argument(
        "--run-id",
        required=True,
        help="Identificador lógico da execução.",
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


def build_sql_string_list(values) -> str:

    return ", ".join(
        (
            "'"
            +
            escape_sql_literal(value)
            +
            "'"
        )
        for value in values
    )


# ============================================================
# RUN
# ============================================================

def get_run(
    spark: SparkSession,
    catalog: str,
    run_id: str,
):

    run_literal = escape_sql_literal(
        run_id
    )

    rows = spark.sql(
        f"""
        SELECT
            run_id,
            status,
            started_at,
            completed_at

        FROM
            `{catalog}`.`{CONTROL_SCHEMA}`.`{RUN_TABLE}`

        WHERE
            run_id = '{run_literal}'
        """
    ).collect()

    if len(rows) != 1:

        raise RuntimeError(
            "Esperado exatamente um ingestion_run. "
            f"run_id={run_id}, "
            f"encontrado={len(rows)}"
        )

    run = rows[0]

    if run["status"] not in {
        "RUNNING",
        "SUCCEEDED",
    }:

        raise RuntimeError(
            "Run não está disponível para commit de estado. "
            f"status={run['status']}"
        )

    return run


# ============================================================
# ÚLTIMA TENTATIVA POR OBJETO
# ============================================================

def get_latest_run_objects(
    spark: SparkSession,
    catalog: str,
    run_id: str,
):
    """
    Obtém somente a tentativa mais recente
    de cada source_object_key.
    """

    run_literal = escape_sql_literal(
        run_id
    )

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
            ro.run_id,
            ro.source_object_key,

            ro.attempt_number,
            ro.load_mode,
            ro.status,

            ro.watermark_column,
            ro.watermark_lower_bound,
            ro.watermark_upper_bound,

            ro.source_row_count,
            ro.landing_row_count,

            ro.state_version_before,
            ro.state_version_after,
            ro.state_committed,

            ro.started_at,
            ro.completed_at,

            so.source_table,

            st.bootstrap_completed,
            st.bootstrap_completed_at,

            st.last_successful_watermark,
            st.last_successful_run_id,
            st.last_successful_load_mode,

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

    if not rows:

        raise RuntimeError(
            "Nenhum objeto encontrado para "
            f"run_id={run_id}"
        )

    return rows


# ============================================================
# CLASSIFICAÇÃO DO ESTADO
# ============================================================

def validate_commit_preconditions(
    rows,
    run_id: str,
):
    """
    Existem três situações legítimas:

    READY
        Estado ainda está exatamente na versão
        observada pelo planner.

    STATE_COMMITTED
        ingestion_state já foi avançado por este
        mesmo run, mas o ledger ainda pode precisar
        ser finalizado.

    FINALIZED
        Estado e ledger já foram confirmados.

    Qualquer outro estado representa conflito.
    """

    ready = []
    state_committed = []
    finalized = []
    errors = []

    for row in rows:

        source_table = row[
            "source_table"
        ]

        before = int(
            row[
                "state_version_before"
            ]
        )

        current = int(
            row[
                "current_state_version"
            ]
        )

        expected_after = (
            before + 1
        )

        # ----------------------------------------------------
        # Evidência básica
        # ----------------------------------------------------

        if (
            row["source_row_count"]
            is None
            or
            row["landing_row_count"]
            is None
        ):

            errors.append(
                f"{source_table}: "
                "contagens ausentes"
            )

            continue

        if (
            row["source_row_count"]
            !=
            row["landing_row_count"]
        ):

            errors.append(
                f"{source_table}: "
                "source_row_count != landing_row_count"
            )

            continue

        # ----------------------------------------------------
        # FINALIZED
        # ----------------------------------------------------

        if (
            row["status"] == "SUCCEEDED"
            and
            row["state_committed"] is True
            and
            row["state_version_after"]
            == expected_after
            and
            current == expected_after
            and
            row["last_successful_run_id"]
            == run_id
        ):

            finalized.append(
                row
            )

            continue

        # ----------------------------------------------------
        # STATE_COMMITTED
        #
        # Útil caso o MERGE do estado tenha terminado,
        # mas a atualização do ledger tenha falhado.
        # ----------------------------------------------------

        if (
            current == expected_after
            and
            row["last_successful_run_id"]
            == run_id
        ):

            state_committed.append(
                row
            )

            continue

        # ----------------------------------------------------
        # READY
        # ----------------------------------------------------

        if (
            row["status"] == "RECONCILED"
            and
            row["state_committed"] is False
            and
            current == before
        ):

            # Objeto com contrato incremental precisa
            # possuir upper bound antes de avançar watermark.

            if (
                row["watermark_column"]
                is not None
                and
                row["watermark_upper_bound"]
                is None
            ):

                errors.append(
                    f"{source_table}: "
                    "watermark_column existe mas "
                    "watermark_upper_bound está NULL"
                )

                continue

            ready.append(
                row
            )

            continue

        # ----------------------------------------------------
        # Qualquer outra combinação é conflito.
        # ----------------------------------------------------

        errors.append(
            (
                f"{source_table}: "
                f"status={row['status']}, "
                f"state_committed="
                f"{row['state_committed']}, "
                f"state_version_before={before}, "
                f"current_state_version={current}, "
                f"last_successful_run_id="
                f"{row['last_successful_run_id']}"
            )
        )

    if errors:

        raise RuntimeError(
            "Pré-condições inválidas para commit: "
            + "; ".join(errors)
        )

    # --------------------------------------------------------
    # O MERGE do ingestion_state é uma única transação.
    #
    # Portanto não esperamos parte dos objetos READY
    # e outra parte já STATE_COMMITTED.
    # --------------------------------------------------------

    if (
        ready
        and
        (
            state_committed
            or
            finalized
        )
    ):

        raise RuntimeError(
            "Estado inconsistente: parte dos objetos ainda "
            "está READY e parte já foi commitada. "
            "O commit do ingestion_state deve ser tratado "
            "como uma unidade."
        )

    print(
        "Estado antes do commit:"
    )

    print(
        f"  READY.............: "
        f"{len(ready)}"
    )

    print(
        f"  STATE_COMMITTED...: "
        f"{len(state_committed)}"
    )

    print(
        f"  FINALIZED.........: "
        f"{len(finalized)}"
    )

    return {
        "ready": ready,
        "state_committed": (
            state_committed
        ),
        "finalized": finalized,
    }


# ============================================================
# COMMIT ATÔMICO DO INGESTION_STATE
# ============================================================

def commit_state(
    spark: SparkSession,
    catalog: str,
    run_id: str,
):
    """
    Um único MERGE modifica ingestion_state para todos
    os objetos da execução.

    O source do MERGE somente produz linhas se TODOS
    os objetos ainda estiverem na versão esperada.

    Assim, uma inconsistência de state_version faz o
    source ficar vazio e nenhuma linha é alterada.
    """

    run_literal = escape_sql_literal(
        run_id
    )

    spark.sql(
        f"""
        MERGE INTO
            `{catalog}`.`{CONTROL_SCHEMA}`.`{STATE_TABLE}`
            AS st

        USING
        (
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
            ),

            latest_attempts AS
            (
                SELECT
                    *

                FROM
                    ranked_attempts

                WHERE
                    rn = 1
            ),

            eligibility AS
            (
                SELECT

                    COUNT(*) AS total_objects,

                    SUM(
                        CASE

                            WHEN
                                ro.status = 'RECONCILED'

                                AND
                                ro.state_committed = FALSE

                                AND
                                st.state_version
                                =
                                ro.state_version_before

                            THEN 1

                            ELSE 0

                        END
                    ) AS eligible_objects

                FROM
                    latest_attempts AS ro

                INNER JOIN
                    `{catalog}`.`{CONTROL_SCHEMA}`.`{STATE_TABLE}`
                    AS st

                    ON
                        st.source_object_key
                        =
                        ro.source_object_key
            )

            SELECT

                ro.source_object_key,

                ro.run_id,

                ro.load_mode,

                ro.watermark_column,

                ro.watermark_upper_bound,

                ro.landing_row_count,

                ro.started_at,

                ro.state_version_before

            FROM
                latest_attempts AS ro

            CROSS JOIN
                eligibility AS e

            WHERE
                e.total_objects
                =
                e.eligible_objects

                AND
                e.total_objects > 0

        ) AS src

        ON
            st.source_object_key
            =
            src.source_object_key

            AND
            st.state_version
            =
            src.state_version_before

        WHEN MATCHED THEN

            UPDATE SET

                st.bootstrap_completed =
                    TRUE,

                st.bootstrap_completed_at =
                    CASE

                        WHEN
                            st.bootstrap_completed

                        THEN
                            st.bootstrap_completed_at

                        ELSE
                            current_timestamp()

                    END,

                st.last_successful_watermark =
                    CASE

                        WHEN
                            src.watermark_column
                            IS NOT NULL

                        THEN
                            src.watermark_upper_bound

                        ELSE
                            st.last_successful_watermark

                    END,

                st.last_successful_run_id =
                    src.run_id,

                st.last_successful_load_mode =
                    src.load_mode,

                st.last_successful_started_at =
                    src.started_at,

                st.last_successful_completed_at =
                    current_timestamp(),

                st.last_successful_row_count =
                    src.landing_row_count,

                st.state_version =
                    st.state_version + 1,

                st.updated_at =
                    current_timestamp()
        """
    )


# ============================================================
# VALIDAÇÃO DO COMMIT DO ESTADO
# ============================================================

def validate_state_commit(
    spark: SparkSession,
    catalog: str,
    run_id: str,
    expected_count: int,
):
    """
    Confirma que todos os objetos:

      last_successful_run_id = run atual
      state_version = before + 1
      bootstrap_completed = true
    """

    run_literal = escape_sql_literal(
        run_id
    )

    summary = spark.sql(
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

            COUNT(*) AS total,

            SUM(
                CASE

                    WHEN
                        st.last_successful_run_id
                        =
                        ro.run_id

                        AND
                        st.state_version
                        =
                        ro.state_version_before + 1

                        AND
                        st.bootstrap_completed = TRUE

                    THEN 1

                    ELSE 0

                END
            ) AS committed

        FROM
            ranked_attempts AS ro

        INNER JOIN
            `{catalog}`.`{CONTROL_SCHEMA}`.`{STATE_TABLE}`
            AS st

            ON
                st.source_object_key
                =
                ro.source_object_key

        WHERE
            ro.rn = 1
        """
    ).first()

    total = int(
        summary["total"]
        or 0
    )

    committed = int(
        summary["committed"]
        or 0
    )

    if total != expected_count:

        raise RuntimeError(
            "Quantidade inesperada de objetos "
            "na validação do state commit. "
            f"Esperado={expected_count}, "
            f"encontrado={total}"
        )

    if committed != expected_count:

        raise RuntimeError(
            "Commit de ingestion_state não foi confirmado "
            "para todos os objetos. "
            f"Esperado={expected_count}, "
            f"confirmado={committed}"
        )

    print(
        f"ingestion_state confirmado para "
        f"{committed} objeto(s)."
    )


# ============================================================
# FINALIZAÇÃO DOS RUN_OBJECTS
# ============================================================

def finalize_run_objects(
    spark: SparkSession,
    catalog: str,
    rows,
):
    """
    Depois que ingestion_state está confirmado:

        RECONCILED -> SUCCEEDED
        state_committed = true
        state_version_after = before + 1

    Também funciona de forma idempotente caso o estado
    já tenha sido commitado em execução anterior deste Job.
    """

    run_object_ids = [
        row[
            "run_object_id"
        ]
        for row in rows
    ]

    run_object_filter = (
        build_sql_string_list(
            run_object_ids
        )
    )

    spark.sql(
        f"""
        UPDATE
            `{catalog}`.`{CONTROL_SCHEMA}`.`{RUN_OBJECT_TABLE}`

        SET
            status =
                'SUCCEEDED',

            state_version_after =
                state_version_before + 1,

            state_committed =
                TRUE,

            completed_at =
                current_timestamp(),

            updated_at =
                current_timestamp()

        WHERE
            run_object_id IN (
                {run_object_filter}
            )

            AND status IN (
                'RECONCILED',
                'SUCCEEDED'
            )
        """
    )


# ============================================================
# VALIDAÇÃO DO LEDGER
# ============================================================

def validate_run_objects_finalized(
    spark: SparkSession,
    catalog: str,
    run_id: str,
    expected_count: int,
):
    """
    Confirma estado final do ledger por objeto.
    """

    run_literal = escape_sql_literal(
        run_id
    )

    summary = spark.sql(
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

            COUNT(*) AS total,

            SUM(
                CASE

                    WHEN
                        status = 'SUCCEEDED'

                        AND
                        state_committed = TRUE

                        AND
                        state_version_after
                        =
                        state_version_before + 1

                    THEN 1

                    ELSE 0

                END
            ) AS succeeded

        FROM
            ranked_attempts

        WHERE
            rn = 1
        """
    ).first()

    total = int(
        summary["total"]
        or 0
    )

    succeeded = int(
        summary["succeeded"]
        or 0
    )

    if total != expected_count:

        raise RuntimeError(
            "Quantidade inesperada de run_objects. "
            f"Esperado={expected_count}, "
            f"encontrado={total}"
        )

    if succeeded != expected_count:

        raise RuntimeError(
            "Nem todos os run_objects foram finalizados. "
            f"Esperado={expected_count}, "
            f"SUCCEEDED={succeeded}"
        )

    print(
        f"run_objects finalizados: "
        f"{succeeded}"
    )


# ============================================================
# FINALIZAÇÃO DO RUN
# ============================================================

def finalize_run(
    spark: SparkSession,
    catalog: str,
    run_id: str,
):
    """
    O run pai somente vira SUCCEEDED depois que:

      Landing materializada
      reconciliação aprovada
      ingestion_state commitado
      todos run_objects SUCCEEDED
    """

    run_literal = escape_sql_literal(
        run_id
    )

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

            AND status IN (
                'RUNNING',
                'SUCCEEDED'
            )
        """
    )


# ============================================================
# RESUMO FINAL
# ============================================================

def print_final_summary(
    spark: SparkSession,
    catalog: str,
    run_id: str,
):
    """
    Mostra o checkpoint consolidado.
    """

    run_literal = escape_sql_literal(
        run_id
    )

    summary = spark.sql(
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

            COUNT(*) AS total_objects,

            SUM(
                CASE
                    WHEN ro.status = 'SUCCEEDED'
                    THEN 1
                    ELSE 0
                END
            ) AS succeeded_objects,

            SUM(
                CASE
                    WHEN ro.state_committed
                    THEN 1
                    ELSE 0
                END
            ) AS committed_objects,

            SUM(
                CASE
                    WHEN st.bootstrap_completed
                    THEN 1
                    ELSE 0
                END
            ) AS bootstrap_completed,

            SUM(
                CASE
                    WHEN
                        st.last_successful_watermark
                        IS NOT NULL

                    THEN 1

                    ELSE 0
                END
            ) AS initialized_watermarks,

            MIN(
                st.state_version
            ) AS min_state_version,

            MAX(
                st.state_version
            ) AS max_state_version

        FROM
            ranked_attempts AS ro

        INNER JOIN
            `{catalog}`.`{CONTROL_SCHEMA}`.`{STATE_TABLE}`
            AS st

            ON
                st.source_object_key
                =
                ro.source_object_key

        WHERE
            ro.rn = 1
        """
    ).first()

    run = get_run(
        spark=spark,
        catalog=catalog,
        run_id=run_id,
    )

    print(
        "============================================================"
    )

    print(
        "COMMIT DO INGESTION_STATE"
    )

    print(
        "============================================================"
    )

    print(
        f"objetos.................: "
        f"{summary['total_objects']}"
    )

    print(
        f"SUCCEEDED...............: "
        f"{summary['succeeded_objects']}"
    )

    print(
        f"state_committed.........: "
        f"{summary['committed_objects']}"
    )

    print(
        f"bootstrap_completed.....: "
        f"{summary['bootstrap_completed']}"
    )

    print(
        f"watermarks inicializados: "
        f"{summary['initialized_watermarks']}"
    )

    print(
        f"state_version min.......: "
        f"{summary['min_state_version']}"
    )

    print(
        f"state_version max.......: "
        f"{summary['max_state_version']}"
    )

    print(
        f"run_status..............: "
        f"{run['status']}"
    )

    print(
        "============================================================"
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

    spark = (
        SparkSession
        .builder
        .getOrCreate()
    )

    print(
        "============================================================"
    )

    print(
        "COMMIT DO CHECKPOINT OPERACIONAL"
    )

    print(
        f"run_id.....: {run_id}"
    )

    print(
        f"Controller.: "
        f"{catalog}.{CONTROL_SCHEMA}"
    )

    print(
        "============================================================"
    )

    # --------------------------------------------------------
    # 1. Run
    # --------------------------------------------------------

    run = get_run(
        spark=spark,
        catalog=catalog,
        run_id=run_id,
    )

    # --------------------------------------------------------
    # 2. Tentativa atual de cada objeto
    # --------------------------------------------------------

    rows = get_latest_run_objects(
        spark=spark,
        catalog=catalog,
        run_id=run_id,
    )

    # --------------------------------------------------------
    # 3. Pré-condições / idempotência
    # --------------------------------------------------------

    classification = (
        validate_commit_preconditions(
            rows=rows,
            run_id=run_id,
        )
    )

    ready = classification[
        "ready"
    ]

    state_committed = classification[
        "state_committed"
    ]

    finalized = classification[
        "finalized"
    ]

    # --------------------------------------------------------
    # 4. Se tudo já estiver finalizado, apenas validamos.
    # --------------------------------------------------------

    if (
        len(finalized)
        ==
        len(rows)
    ):

        print(
            "Run já estava completamente finalizado. "
            "Nenhuma alteração adicional necessária."
        )

        print_final_summary(
            spark=spark,
            catalog=catalog,
            run_id=run_id,
        )

        return

    # --------------------------------------------------------
    # 5. Primeiro commit deste run
    # --------------------------------------------------------

    if ready:

        commit_state(
            spark=spark,
            catalog=catalog,
            run_id=run_id,
        )

    # --------------------------------------------------------
    # 6. Ou recuperação idempotente:
    #
    # ingestion_state já foi commitado por este run,
    # mas o ledger ainda não foi completamente finalizado.
    # --------------------------------------------------------

    elif state_committed:

        print(
            "ingestion_state já foi confirmado por este run. "
            "Continuando apenas a finalização do ledger."
        )

    # --------------------------------------------------------
    # 7. Confirma checkpoint
    # --------------------------------------------------------

    validate_state_commit(
        spark=spark,
        catalog=catalog,
        run_id=run_id,
        expected_count=len(rows),
    )

    # --------------------------------------------------------
    # 8. Ledger por objeto
    # --------------------------------------------------------

    finalize_run_objects(
        spark=spark,
        catalog=catalog,
        rows=rows,
    )

    validate_run_objects_finalized(
        spark=spark,
        catalog=catalog,
        run_id=run_id,
        expected_count=len(rows),
    )

    # --------------------------------------------------------
    # 9. Envelope da execução
    # --------------------------------------------------------

    finalize_run(
        spark=spark,
        catalog=catalog,
        run_id=run_id,
    )

    # --------------------------------------------------------
    # 10. Resultado
    # --------------------------------------------------------

    print_final_summary(
        spark=spark,
        catalog=catalog,
        run_id=run_id,
    )

    print(
        "Checkpoint operacional confirmado com sucesso."
    )


if __name__ == "__main__":
    main()
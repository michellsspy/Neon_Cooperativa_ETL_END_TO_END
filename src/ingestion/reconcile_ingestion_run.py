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


# ============================================================
# ARGUMENTOS
# ============================================================

def parse_args():

    parser = argparse.ArgumentParser(
        description=(
            "Executa a reconciliação formal de uma execução "
            "de ingestão já materializada na Landing."
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
        help="Identificador lógico da execução de ingestão.",
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

    if run["status"] != "RUNNING":

        raise RuntimeError(
            "A reconciliação exige ingestion_run RUNNING. "
            f"status atual={run['status']}"
        )

    return run


# ============================================================
# TENTATIVA ATUAL DE CADA OBJETO
# ============================================================

def get_latest_run_objects(
    spark: SparkSession,
    catalog: str,
    run_id: str,
):
    """
    Recupera apenas a tentativa mais recente de cada objeto.

    Isso já deixa a reconciliação preparada para o modelo
    histórico de attempts sem considerar tentativas antigas.
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
                ro.run_id = '{run_literal}'
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

            ro.landing_data_path,
            ro.manifest_path,

            ro.state_version_before,
            ro.state_version_after,
            ro.state_committed,

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

    if not rows:

        raise RuntimeError(
            "Nenhum objeto encontrado para "
            f"run_id={run_id}"
        )

    return rows


# ============================================================
# VALIDAÇÃO DO LEDGER
# ============================================================

def validate_controller_state(
    rows,
):
    """
    Antes de acessar qualquer arquivo físico, todos os objetos
    atuais precisam estar MATERIALIZED e ainda sem commit
    de ingestion_state.
    """

    errors = []

    for row in rows:

        source_table = row[
            "source_table"
        ]

        if row["status"] != "MATERIALIZED":

            errors.append(
                f"{source_table}: "
                f"status={row['status']}"
            )

        if row["state_committed"]:

            errors.append(
                f"{source_table}: "
                "state_committed=true"
            )

        if row["source_row_count"] is None:

            errors.append(
                f"{source_table}: "
                "source_row_count ausente"
            )

        if row["landing_row_count"] is None:

            errors.append(
                f"{source_table}: "
                "landing_row_count ausente"
            )

        if row["landing_data_path"] is None:

            errors.append(
                f"{source_table}: "
                "landing_data_path ausente"
            )

        if row["manifest_path"] is None:

            errors.append(
                f"{source_table}: "
                "manifest_path ausente"
            )

        if (
            row["source_row_count"]
            is not None
            and
            row["landing_row_count"]
            is not None
            and
            row["source_row_count"]
            !=
            row["landing_row_count"]
        ):

            errors.append(
                f"{source_table}: "
                "source_row_count diferente de "
                "landing_row_count"
            )

    if errors:

        raise RuntimeError(
            "Controller não está pronto para reconciliação: "
            + "; ".join(errors)
        )

    print(
        f"Controller validado: "
        f"{len(rows)} objeto(s) MATERIALIZED."
    )


# ============================================================
# VALIDAÇÃO DO MANIFEST
# ============================================================

def validate_manifest(
    spark: SparkSession,
    row,
    physical_landing_count: int,
):
    """
    Confirma que o sidecar pertence exatamente à tentativa
    que está sendo reconciliada.
    """

    manifest_path = row[
        "manifest_path"
    ]

    manifest_df = (
        spark.read
        .format("json")
        .load(
            manifest_path
        )
    )

    manifest_count = int(
        manifest_df.count()
    )

    if manifest_count != 1:

        raise RuntimeError(
            f"{row['source_table']}: "
            "manifest deve conter exatamente uma linha. "
            f"Encontrado={manifest_count}"
        )

    manifest = (
        manifest_df.first()
    )

    required_columns = {
        "run_id",
        "run_object_id",
        "source_object_key",
        "attempt_number",
        "source_row_count",
        "landing_row_count",
        "landing_data_path",
    }

    missing_columns = (
        required_columns
        -
        set(
            manifest_df.columns
        )
    )

    if missing_columns:

        raise RuntimeError(
            f"{row['source_table']}: "
            "manifest incompleto. "
            f"Colunas ausentes="
            f"{sorted(missing_columns)}"
        )

    if (
        manifest["run_id"]
        !=
        row["run_id"]
    ):

        raise RuntimeError(
            f"{row['source_table']}: "
            "manifest.run_id divergente."
        )

    if (
        manifest["run_object_id"]
        !=
        row["run_object_id"]
    ):

        raise RuntimeError(
            f"{row['source_table']}: "
            "manifest.run_object_id divergente."
        )

    if (
        manifest["source_object_key"]
        !=
        row["source_object_key"]
    ):

        raise RuntimeError(
            f"{row['source_table']}: "
            "manifest.source_object_key divergente."
        )

    if (
        int(
            manifest[
                "attempt_number"
            ]
        )
        !=
        int(
            row[
                "attempt_number"
            ]
        )
    ):

        raise RuntimeError(
            f"{row['source_table']}: "
            "manifest.attempt_number divergente."
        )

    if (
        int(
            manifest[
                "source_row_count"
            ]
        )
        !=
        int(
            row[
                "source_row_count"
            ]
        )
    ):

        raise RuntimeError(
            f"{row['source_table']}: "
            "manifest.source_row_count divergente."
        )

    if (
        int(
            manifest[
                "landing_row_count"
            ]
        )
        !=
        int(
            row[
                "landing_row_count"
            ]
        )
    ):

        raise RuntimeError(
            f"{row['source_table']}: "
            "manifest.landing_row_count divergente."
        )

    if (
        int(
            manifest[
                "landing_row_count"
            ]
        )
        !=
        physical_landing_count
    ):

        raise RuntimeError(
            f"{row['source_table']}: "
            "manifest diverge da contagem física."
        )

    if (
        manifest[
            "landing_data_path"
        ]
        !=
        row[
            "landing_data_path"
        ]
    ):

        raise RuntimeError(
            f"{row['source_table']}: "
            "manifest.landing_data_path divergente."
        )


# ============================================================
# RECONCILIAÇÃO FÍSICA
# ============================================================

def reconcile_object(
    spark: SparkSession,
    row,
):
    """
    Executa validação física independente sobre a Landing.

    Não consulta novamente a origem.
    """

    source_table = row[
        "source_table"
    ]

    data_path = row[
        "landing_data_path"
    ]

    print(
        "------------------------------------------------------------"
    )

    print(
        f"Reconciliando: "
        f"{source_table}"
    )

    landing_df = (
        spark.read
        .format("parquet")
        .load(
            data_path
        )
    )

    physical_landing_count = int(
        landing_df.count()
    )

    controller_source_count = int(
        row[
            "source_row_count"
        ]
    )

    controller_landing_count = int(
        row[
            "landing_row_count"
        ]
    )

    print(
        f"  source ledger....: "
        f"{controller_source_count}"
    )

    print(
        f"  landing ledger...: "
        f"{controller_landing_count}"
    )

    print(
        f"  landing físico...: "
        f"{physical_landing_count}"
    )

    if (
        controller_source_count
        !=
        controller_landing_count
    ):

        raise RuntimeError(
            f"{source_table}: "
            "divergência no ledger "
            "source x landing."
        )

    if (
        controller_landing_count
        !=
        physical_landing_count
    ):

        raise RuntimeError(
            f"{source_table}: "
            "divergência Controller x "
            "Landing física."
        )

    validate_manifest(
        spark=spark,
        row=row,
        physical_landing_count=(
            physical_landing_count
        ),
    )

    print(
        "  manifest.........: OK"
    )

    print(
        "  resultado........: OK"
    )


# ============================================================
# COMMIT DA RECONCILIAÇÃO
# ============================================================

def commit_reconciliation(
    spark: SparkSession,
    catalog: str,
    rows,
):
    """
    Somente depois que TODOS os objetos passam pela validação
    física fazemos uma única transição lógica:

        MATERIALIZED -> RECONCILED

    ingestion_state continua intocado.
    """

    run_object_ids = [
        row[
            "run_object_id"
        ]
        for row in rows
    ]

    run_object_filter = ", ".join(
        (
            "'"
            +
            escape_sql_literal(
                run_object_id
            )
            +
            "'"
        )
        for run_object_id
        in run_object_ids
    )

    spark.sql(
        f"""
        UPDATE
            `{catalog}`.`{CONTROL_SCHEMA}`.`{RUN_OBJECT_TABLE}`

        SET
            status =
                'RECONCILED',

            updated_at =
                current_timestamp()

        WHERE
            run_object_id IN (
                {run_object_filter}
            )

            AND status =
                'MATERIALIZED'

            AND state_committed =
                FALSE
        """
    )


# ============================================================
# VALIDAÇÃO FINAL
# ============================================================

def validate_final_state(
    spark: SparkSession,
    catalog: str,
    run_id: str,
    expected_count: int,
):
    """
    Confirma o resultado do microprocesso.

    O run pai permanece RUNNING.
    O ingestion_state permanece sem commit.
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
                    WHEN status = 'RECONCILED'
                    THEN 1
                    ELSE 0
                END
            ) AS reconciled,

            SUM(
                CASE
                    WHEN state_committed
                    THEN 1
                    ELSE 0
                END
            ) AS state_committed

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

    reconciled = int(
        summary["reconciled"]
        or 0
    )

    state_committed = int(
        summary["state_committed"]
        or 0
    )

    if total != expected_count:

        raise RuntimeError(
            "Quantidade inesperada de objetos. "
            f"Esperado={expected_count}, "
            f"encontrado={total}"
        )

    if reconciled != expected_count:

        raise RuntimeError(
            "Nem todos os objetos terminaram "
            "RECONCILED. "
            f"Esperado={expected_count}, "
            f"encontrado={reconciled}"
        )

    if state_committed != 0:

        raise RuntimeError(
            "ingestion_state foi commitado "
            "prematuramente."
        )

    run = get_run(
        spark=spark,
        catalog=catalog,
        run_id=run_id,
    )

    print(
        "============================================================"
    )

    print(
        "RESUMO DA RECONCILIAÇÃO"
    )

    print(
        "============================================================"
    )

    print(
        f"objetos..............: "
        f"{total}"
    )

    print(
        f"RECONCILED...........: "
        f"{reconciled}"
    )

    print(
        f"state_committed......: "
        f"{state_committed}"
    )

    print(
        f"run_status...........: "
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
        "RECONCILIAÇÃO FORMAL DA LANDING"
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
    # 1. Run precisa estar operacionalmente aberto
    # --------------------------------------------------------

    get_run(
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
    # 3. Ledger precisa estar consistente
    # --------------------------------------------------------

    validate_controller_state(
        rows=rows,
    )

    # --------------------------------------------------------
    # 4. Reconciliação física
    #
    # Nenhum status é alterado enquanto existirem
    # objetos ainda não validados.
    # --------------------------------------------------------

    for row in rows:

        reconcile_object(
            spark=spark,
            row=row,
        )

    # --------------------------------------------------------
    # 5. Todos passaram:
    # MATERIALIZED -> RECONCILED
    # --------------------------------------------------------

    commit_reconciliation(
        spark=spark,
        catalog=catalog,
        rows=rows,
    )

    # --------------------------------------------------------
    # 6. Gate final
    # --------------------------------------------------------

    validate_final_state(
        spark=spark,
        catalog=catalog,
        run_id=run_id,
        expected_count=len(rows),
    )

    print(
        "Reconciliação concluída com sucesso."
    )

    print(
        "ingestion_state NÃO foi alterado."
    )


if __name__ == "__main__":
    main()
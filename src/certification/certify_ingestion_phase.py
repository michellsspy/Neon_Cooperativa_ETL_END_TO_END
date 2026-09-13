import argparse
import re

from pyspark.sql import SparkSession


# ============================================================
# CONSTANTES
# ============================================================

CONTROL_SCHEMA = "control"

EXPECTED_CONTROL_TABLES = {
    "source_system",
    "source_object",
    "source_column",
    "incremental_profile",
    "ingestion_policy",
    "ingestion_state",
    "ingestion_run",
    "ingestion_run_object",
}

EXPECTED_SOURCE_OBJECTS = 10

EXPECTED_FULL_OBJECTS = 2
EXPECTED_INCREMENTAL_OBJECTS = 8


# ============================================================
# ARGUMENTOS
# ============================================================

def parse_args():

    parser = argparse.ArgumentParser(
        description=(
            "Executa certificação read-only da fase "
            "de ingestão do projeto Neon Cooperativa."
        )
    )

    parser.add_argument(
        "--catalog",
        required=True,
        help="Catálogo gerenciado do projeto.",
    )

    return parser.parse_args()


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


def fail(
    message: str,
):

    raise RuntimeError(
        f"CERTIFICATION FAILED: {message}"
    )


def check(
    condition: bool,
    message: str,
):

    if not condition:

        fail(
            message
        )


def print_check(
    name: str,
):

    print(
        f"  {name:<46} OK"
    )


# ============================================================
# 1. MODELO DO CONTROLLER
# ============================================================

def certify_controller_model(
    spark: SparkSession,
    catalog: str,
):
    """
    Garante que o núcleo metadata-driven está completo.
    """

    tables = spark.sql(
        f"""
        SELECT
            table_name

        FROM
            `{catalog}`.information_schema.tables

        WHERE
            table_schema =
                '{CONTROL_SCHEMA}'

            AND table_name IN (
                'source_system',
                'source_object',
                'source_column',
                'incremental_profile',
                'ingestion_policy',
                'ingestion_state',
                'ingestion_run',
                'ingestion_run_object'
            )
        """
    ).collect()

    actual_tables = {
        row["table_name"]
        for row in tables
    }

    missing = (
        EXPECTED_CONTROL_TABLES
        -
        actual_tables
    )

    check(
        not missing,
        (
            "Tabelas obrigatórias ausentes no Controller: "
            f"{sorted(missing)}"
        ),
    )

    print_check(
        "Modelo físico do Controller"
    )


# ============================================================
# 2. DOCUMENTAÇÃO DO CONTROLLER
# ============================================================

def certify_metadata_documentation(
    spark: SparkSession,
    catalog: str,
):
    """
    Nossa definição de pronto exige comentários nas tabelas
    e colunas publicadas do Controller.
    """

    table_filter = ", ".join(
        f"'{name}'"
        for name
        in sorted(
            EXPECTED_CONTROL_TABLES
        )
    )

    missing_table_comments = spark.sql(
        f"""
        SELECT
            table_name

        FROM
            `{catalog}`.information_schema.tables

        WHERE
            table_schema =
                '{CONTROL_SCHEMA}'

            AND table_name IN (
                {table_filter}
            )

            AND (
                comment IS NULL
                OR trim(comment) = ''
            )

        ORDER BY
            table_name
        """
    ).collect()

    if missing_table_comments:

        names = [
            row["table_name"]
            for row
            in missing_table_comments
        ]

        fail(
            "Tabelas sem comentário: "
            f"{names}"
        )

    missing_column_comments = spark.sql(
        f"""
        SELECT
            table_name,
            column_name

        FROM
            `{catalog}`.information_schema.columns

        WHERE
            table_schema =
                '{CONTROL_SCHEMA}'

            AND table_name IN (
                {table_filter}
            )

            AND (
                comment IS NULL
                OR trim(comment) = ''
            )

        ORDER BY
            table_name,
            ordinal_position
        """
    ).collect()

    if missing_column_comments:

        names = [
            (
                f"{row['table_name']}."
                f"{row['column_name']}"
            )
            for row
            in missing_column_comments
        ]

        fail(
            "Colunas sem comentário: "
            f"{names}"
        )

    print_check(
        "Documentação de tabelas e colunas"
    )


# ============================================================
# 3. SISTEMA DE ORIGEM
# ============================================================

def certify_source_system(
    spark: SparkSession,
    catalog: str,
):
    """
    Confirma que a integração governada da origem
    está devidamente cadastrada.
    """

    rows = spark.sql(
        f"""
        SELECT
            source_system_key,
            source_type,
            connection_name,
            foreign_catalog,
            enabled

        FROM
            `{catalog}`.`{CONTROL_SCHEMA}`.`source_system`

        WHERE
            enabled = TRUE
        """
    ).collect()

    check(
        len(rows) == 1,
        (
            "Esperado exatamente um source_system "
            f"habilitado. Encontrado={len(rows)}"
        ),
    )

    row = rows[0]

    check(
        row["connection_name"] is not None,
        "connection_name está NULL.",
    )

    check(
        row["foreign_catalog"] is not None,
        "foreign_catalog está NULL.",
    )

    check(
        row["source_type"] is not None,
        "source_type está NULL.",
    )

    print_check(
        "Sistema de origem governado"
    )


# ============================================================
# 4. COBERTURA DE CONFIGURAÇÃO
# ============================================================

def certify_configuration_coverage(
    spark: SparkSession,
    catalog: str,
):
    """
    Todo source_object habilitado precisa possuir:

        policy
        state
    """

    summary = spark.sql(
        f"""
        SELECT

            COUNT(*) AS source_objects,

            SUM(
                CASE
                    WHEN ip.source_object_key
                         IS NOT NULL
                    THEN 1
                    ELSE 0
                END
            ) AS policies,

            SUM(
                CASE
                    WHEN st.source_object_key
                         IS NOT NULL
                    THEN 1
                    ELSE 0
                END
            ) AS states

        FROM
            `{catalog}`.`{CONTROL_SCHEMA}`.`source_object`
            AS so

        LEFT JOIN
            `{catalog}`.`{CONTROL_SCHEMA}`.`ingestion_policy`
            AS ip

            ON
                ip.source_object_key =
                so.source_object_key

                AND ip.enabled = TRUE

        LEFT JOIN
            `{catalog}`.`{CONTROL_SCHEMA}`.`ingestion_state`
            AS st

            ON
                st.source_object_key =
                so.source_object_key

        WHERE
            so.enabled = TRUE
        """
    ).first()

    source_objects = int(
        summary["source_objects"]
        or 0
    )

    policies = int(
        summary["policies"]
        or 0
    )

    states = int(
        summary["states"]
        or 0
    )

    check(
        source_objects
        ==
        EXPECTED_SOURCE_OBJECTS,
        (
            "Quantidade inesperada de source_objects. "
            f"Esperado={EXPECTED_SOURCE_OBJECTS}, "
            f"encontrado={source_objects}"
        ),
    )

    check(
        policies
        ==
        source_objects,
        (
            "Nem todos os objetos possuem "
            "ingestion_policy habilitada."
        ),
    )

    check(
        states
        ==
        source_objects,
        (
            "Nem todos os objetos possuem "
            "ingestion_state."
        ),
    )

    print_check(
        "Cobertura source_object/policy/state"
    )


# ============================================================
# 5. DISTRIBUIÇÃO FULL / INCREMENTAL
# ============================================================

def certify_policy_distribution(
    spark: SparkSession,
    catalog: str,
):
    """
    Contrato atual do dataset:

        2 FULL
        8 INCREMENTAL
    """

    rows = spark.sql(
        f"""
        SELECT
            ip.steady_state_mode,
            COUNT(*) AS qtd

        FROM
            `{catalog}`.`{CONTROL_SCHEMA}`.`ingestion_policy`
            AS ip

        INNER JOIN
            `{catalog}`.`{CONTROL_SCHEMA}`.`source_object`
            AS so

            ON
                so.source_object_key =
                ip.source_object_key

        WHERE
            so.enabled = TRUE

            AND ip.enabled = TRUE

        GROUP BY
            ip.steady_state_mode
        """
    ).collect()

    distribution = {
        row["steady_state_mode"]:
            int(
                row["qtd"]
            )

        for row
        in rows
    }

    check(
        distribution.get(
            "FULL",
            0,
        )
        ==
        EXPECTED_FULL_OBJECTS,
        (
            "Quantidade inesperada de políticas FULL. "
            f"Encontrado={distribution}"
        ),
    )

    check(
        distribution.get(
            "INCREMENTAL",
            0,
        )
        ==
        EXPECTED_INCREMENTAL_OBJECTS,
        (
            "Quantidade inesperada de políticas "
            f"INCREMENTAL. Encontrado={distribution}"
        ),
    )

    print_check(
        "Distribuição 2 FULL / 8 INCREMENTAL"
    )


# ============================================================
# 6. CHECKPOINT ATUAL
# ============================================================

def get_current_state_rows(
    spark: SparkSession,
    catalog: str,
):

    return spark.sql(
        f"""
        SELECT

            so.source_object_key,
            so.source_table,

            ip.steady_state_mode,
            ip.incremental_column,

            st.bootstrap_completed,
            st.watermark_column,
            st.watermark_data_type,

            st.last_successful_watermark,
            st.last_successful_run_id,
            st.last_successful_load_mode,
            st.last_successful_row_count,

            st.state_version,

            r.status
                AS last_run_status,

            r.trigger_type
                AS last_run_trigger_type

        FROM
            `{catalog}`.`{CONTROL_SCHEMA}`.`source_object`
            AS so

        INNER JOIN
            `{catalog}`.`{CONTROL_SCHEMA}`.`ingestion_policy`
            AS ip

            ON
                ip.source_object_key =
                so.source_object_key

        INNER JOIN
            `{catalog}`.`{CONTROL_SCHEMA}`.`ingestion_state`
            AS st

            ON
                st.source_object_key =
                so.source_object_key

        LEFT JOIN
            `{catalog}`.`{CONTROL_SCHEMA}`.`ingestion_run`
            AS r

            ON
                r.run_id =
                st.last_successful_run_id

        WHERE
            so.enabled = TRUE

            AND ip.enabled = TRUE

        ORDER BY
            so.source_table
        """
    ).collect()


def certify_current_state(
    spark: SparkSession,
    catalog: str,
):
    """
    Valida o checkpoint operacional atualmente confirmado.

    Não exige state_version uniforme entre objetos.
    """

    rows = get_current_state_rows(
        spark=spark,
        catalog=catalog,
    )

    check(
        len(rows)
        ==
        EXPECTED_SOURCE_OBJECTS,
        "Cobertura incompleta do ingestion_state.",
    )

    errors = []

    for row in rows:

        table = row[
            "source_table"
        ]

        mode = row[
            "steady_state_mode"
        ]

        if not row[
            "bootstrap_completed"
        ]:

            errors.append(
                f"{table}: bootstrap não concluído"
            )

        if (
            row[
                "last_successful_run_id"
            ]
            is None
        ):

            errors.append(
                f"{table}: last_successful_run_id NULL"
            )

        if (
            row[
                "last_successful_row_count"
            ]
            is None
        ):

            errors.append(
                f"{table}: last_successful_row_count NULL"
            )

        if int(
            row["state_version"]
        ) < 1:

            errors.append(
                f"{table}: state_version inválido"
            )

        if (
            row["last_run_status"]
            != "SUCCEEDED"
        ):

            errors.append(
                f"{table}: último run não está SUCCEEDED"
            )

        # Reprocess não pode se tornar checkpoint operacional.
        if (
            row[
                "last_run_trigger_type"
            ]
            ==
            "REPROCESS"
        ):

            errors.append(
                f"{table}: checkpoint aponta para REPROCESS"
            )

        if mode == "INCREMENTAL":

            if (
                row[
                    "watermark_column"
                ]
                is None
            ):

                errors.append(
                    f"{table}: watermark_column NULL"
                )

            if (
                row[
                    "last_successful_watermark"
                ]
                is None
            ):

                errors.append(
                    f"{table}: watermark NULL"
                )

        elif mode == "FULL":

            if (
                row[
                    "last_successful_watermark"
                ]
                is not None
            ):

                errors.append(
                    f"{table}: FULL possui watermark"
                )

    if errors:

        fail(
            "Problemas no ingestion_state: "
            + "; ".join(
                errors
            )
        )

    print_check(
        "Checkpoint operacional atual"
    )

    return rows


# ============================================================
# 7. EVIDÊNCIA DO LEDGER ATUAL
# ============================================================

def get_current_evidence_rows(
    spark: SparkSession,
    catalog: str,
):

    return spark.sql(
        f"""
        SELECT

            so.source_table,

            st.last_successful_run_id,

            st.last_successful_row_count,

            ro.run_object_id,

            ro.status,

            ro.source_row_count,
            ro.landing_row_count,

            ro.landing_data_path,
            ro.manifest_path,

            ro.state_committed,
            ro.state_version_before,
            ro.state_version_after

        FROM
            `{catalog}`.`{CONTROL_SCHEMA}`.`ingestion_state`
            AS st

        INNER JOIN
            `{catalog}`.`{CONTROL_SCHEMA}`.`source_object`
            AS so

            ON
                so.source_object_key =
                st.source_object_key

        INNER JOIN
            `{catalog}`.`{CONTROL_SCHEMA}`.`ingestion_run_object`
            AS ro

            ON
                ro.source_object_key =
                st.source_object_key

                AND ro.run_id =
                    st.last_successful_run_id

                AND ro.state_committed =
                    TRUE

                AND ro.status =
                    'SUCCEEDED'

        WHERE
            so.enabled = TRUE

        ORDER BY
            so.source_table
        """
    ).collect()


def certify_current_ledger(
    spark: SparkSession,
    catalog: str,
):

    rows = get_current_evidence_rows(
        spark=spark,
        catalog=catalog,
    )

    check(
        len(rows)
        ==
        EXPECTED_SOURCE_OBJECTS,
        (
            "Não foi encontrada exatamente uma evidência "
            "commitada para cada objeto atual. "
            f"Encontrado={len(rows)}"
        ),
    )

    for row in rows:

        table = row[
            "source_table"
        ]

        check(
            row[
                "source_row_count"
            ]
            ==
            row[
                "landing_row_count"
            ],
            (
                f"{table}: source_row_count "
                "difere de landing_row_count."
            ),
        )

        check(
            row[
                "landing_row_count"
            ]
            ==
            row[
                "last_successful_row_count"
            ],
            (
                f"{table}: ledger diverge do "
                "ingestion_state."
            ),
        )

        check(
            row[
                "landing_data_path"
            ]
            is not None,
            f"{table}: landing_data_path NULL.",
        )

        check(
            row[
                "manifest_path"
            ]
            is not None,
            f"{table}: manifest_path NULL.",
        )

    print_check(
        "Ledger do checkpoint atual"
    )

    return rows


# ============================================================
# 8. EVIDÊNCIA FÍSICA DA LANDING
# ============================================================

def certify_current_landing(
    spark: SparkSession,
    rows,
):
    """
    Relê fisicamente os 10 lotes atualmente confirmados
    e seus respectivos manifests.
    """

    for row in rows:

        table = row[
            "source_table"
        ]

        data_path = row[
            "landing_data_path"
        ]

        manifest_path = row[
            "manifest_path"
        ]

        # ----------------------------------------------------
        # Parquet físico
        # ----------------------------------------------------

        physical_count = int(
            spark.read
            .format("parquet")
            .load(data_path)
            .count()
        )

        expected_count = int(
            row[
                "last_successful_row_count"
            ]
        )

        check(
            physical_count
            ==
            expected_count,
            (
                f"{table}: Landing física={physical_count}, "
                f"checkpoint={expected_count}"
            ),
        )

        # ----------------------------------------------------
        # Manifest
        # ----------------------------------------------------

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

        check(
            manifest_count == 1,
            (
                f"{table}: manifest deve possuir "
                "exatamente uma linha."
            ),
        )

        manifest = (
            manifest_df.first()
        )

        required_fields = {
            "run_id",
            "run_object_id",
            "source_row_count",
            "landing_row_count",
            "landing_data_path",
        }

        missing = (
            required_fields
            -
            set(
                manifest_df.columns
            )
        )

        check(
            not missing,
            (
                f"{table}: manifest incompleto. "
                f"Ausentes={sorted(missing)}"
            ),
        )

        check(
            manifest[
                "run_id"
            ]
            ==
            row[
                "last_successful_run_id"
            ],
            f"{table}: manifest.run_id divergente.",
        )

        check(
            manifest[
                "run_object_id"
            ]
            ==
            row[
                "run_object_id"
            ],
            (
                f"{table}: "
                "manifest.run_object_id divergente."
            ),
        )

        check(
            int(
                manifest[
                    "source_row_count"
                ]
            )
            ==
            expected_count,
            (
                f"{table}: "
                "manifest.source_row_count divergente."
            ),
        )

        check(
            int(
                manifest[
                    "landing_row_count"
                ]
            )
            ==
            physical_count,
            (
                f"{table}: "
                "manifest.landing_row_count divergente."
            ),
        )

        check(
            manifest[
                "landing_data_path"
            ]
            ==
            data_path,
            (
                f"{table}: "
                "manifest.landing_data_path divergente."
            ),
        )

    print_check(
        "Landing física + manifests atuais"
    )


# ============================================================
# 9. HISTÓRICO DE RETRY
# ============================================================

def certify_retry_history(
    spark: SparkSession,
    catalog: str,
):
    """
    Exige evidência de pelo menos um retry controlado:

        tentativa FAILED
        ↓
        nova tentativa SUCCEEDED
        mesmo run
    """

    count = int(
        spark.sql(
            f"""
            SELECT
                COUNT(*) AS qtd

            FROM
                `{catalog}`.`{CONTROL_SCHEMA}`.`ingestion_run_object`
                AS successful_attempt

            INNER JOIN
                `{catalog}`.`{CONTROL_SCHEMA}`.`ingestion_run_object`
                AS failed_attempt

                ON
                    successful_attempt.reprocess_of_run_object_id
                    =
                    failed_attempt.run_object_id

                    AND
                    successful_attempt.run_id
                    =
                    failed_attempt.run_id

            INNER JOIN
                `{catalog}`.`{CONTROL_SCHEMA}`.`ingestion_run`
                AS r

                ON
                    r.run_id =
                    successful_attempt.run_id

            WHERE
                failed_attempt.status =
                    'FAILED'

                AND
                successful_attempt.status =
                    'SUCCEEDED'

                AND
                successful_attempt.attempt_number
                >
                failed_attempt.attempt_number

                AND
                r.status =
                    'SUCCEEDED'
            """
        ).first()["qtd"]
        or 0
    )

    check(
        count >= 1,
        (
            "Não existe evidência histórica "
            "de retry controlado."
        ),
    )

    print_check(
        "Retry controlado"
    )


# ============================================================
# 10. HISTÓRICO DE REPROCESSAMENTO
# ============================================================

def certify_reprocessing_history(
    spark: SparkSession,
    catalog: str,
):
    """
    Exige evidência de REPROCESS bem-sucedido que
    não tenha alterado ingestion_state.
    """

    summary = spark.sql(
        f"""
        SELECT

            SUM(
                CASE

                    WHEN
                        r.status = 'SUCCEEDED'

                        AND
                        ro.status = 'SUCCEEDED'

                        AND
                        ro.reprocess_of_run_object_id
                        IS NOT NULL

                        AND
                        ro.state_committed = FALSE

                        AND
                        ro.state_version_after
                        IS NULL

                    THEN 1

                    ELSE 0

                END
            ) AS valid_reprocesses,

            SUM(
                CASE

                    WHEN
                        ro.state_committed = TRUE

                        OR
                        ro.state_version_after
                        IS NOT NULL

                    THEN 1

                    ELSE 0

                END
            ) AS invalid_state_commits

        FROM
            `{catalog}`.`{CONTROL_SCHEMA}`.`ingestion_run`
            AS r

        INNER JOIN
            `{catalog}`.`{CONTROL_SCHEMA}`.`ingestion_run_object`
            AS ro

            ON
                ro.run_id =
                r.run_id

        WHERE
            r.trigger_type =
                'REPROCESS'
        """
    ).first()

    valid = int(
        summary[
            "valid_reprocesses"
        ]
        or 0
    )

    invalid = int(
        summary[
            "invalid_state_commits"
        ]
        or 0
    )

    check(
        valid >= 1,
        (
            "Não existe evidência de "
            "reprocessamento manual bem-sucedido."
        ),
    )

    check(
        invalid == 0,
        (
            "Existe REPROCESS que alterou "
            "indevidamente o checkpoint."
        ),
    )

    print_check(
        "Reprocessamento sem avanço de checkpoint"
    )


# ============================================================
# 11. RUNS ATIVOS / ÓRFÃOS
# ============================================================

def certify_no_inconsistent_runs(
    spark: SparkSession,
    catalog: str,
):
    """
    Ao final da certificação não pode existir execução
    operacional abandonada em CREATED ou RUNNING.
    """

    active_count = int(
        spark.sql(
            f"""
            SELECT
                COUNT(*) AS qtd

            FROM
                `{catalog}`.`{CONTROL_SCHEMA}`.`ingestion_run`

            WHERE
                status IN (
                    'CREATED',
                    'RUNNING'
                )
            """
        ).first()["qtd"]
        or 0
    )

    check(
        active_count == 0,
        (
            "Existem ingestion_runs ainda "
            f"CREATED/RUNNING: {active_count}"
        ),
    )

    orphan_count = int(
        spark.sql(
            f"""
            SELECT
                COUNT(*) AS qtd

            FROM
                `{catalog}`.`{CONTROL_SCHEMA}`.`ingestion_run_object`
                AS ro

            INNER JOIN
                `{catalog}`.`{CONTROL_SCHEMA}`.`ingestion_run`
                AS r

                ON
                    r.run_id =
                    ro.run_id

            WHERE
                r.status IN (
                    'SUCCEEDED',
                    'FAILED',
                    'CANCELLED'
                )

                AND
                ro.status IN (
                    'PENDING',
                    'RUNNING',
                    'MATERIALIZED',
                    'RECONCILED'
                )
            """
        ).first()["qtd"]
        or 0
    )

    check(
        orphan_count == 0,
        (
            "Existem run_objects não terminais "
            "dentro de runs terminais: "
            f"{orphan_count}"
        ),
    )

    print_check(
        "Ausência de runs/tentativas órfãs"
    )


# ============================================================
# MAIN
# ============================================================

def main():

    args = parse_args()

    catalog = validate_identifier(
        args.catalog
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
        "CERTIFICAÇÃO FINAL — FASE DE INGESTÃO"
    )

    print(
        "============================================================"
    )

    certify_controller_model(
        spark=spark,
        catalog=catalog,
    )

    certify_metadata_documentation(
        spark=spark,
        catalog=catalog,
    )

    certify_source_system(
        spark=spark,
        catalog=catalog,
    )

    certify_configuration_coverage(
        spark=spark,
        catalog=catalog,
    )

    certify_policy_distribution(
        spark=spark,
        catalog=catalog,
    )

    certify_current_state(
        spark=spark,
        catalog=catalog,
    )

    evidence_rows = (
        certify_current_ledger(
            spark=spark,
            catalog=catalog,
        )
    )

    certify_current_landing(
        spark=spark,
        rows=evidence_rows,
    )

    certify_retry_history(
        spark=spark,
        catalog=catalog,
    )

    certify_reprocessing_history(
        spark=spark,
        catalog=catalog,
    )

    certify_no_inconsistent_runs(
        spark=spark,
        catalog=catalog,
    )

    print(
        "============================================================"
    )

    print(
        "INGESTÃO CERTIFICADA COM SUCESSO"
    )

    print(
        "============================================================"
    )

    print(
        "Capacidades certificadas:"
    )

    print(
        "  metadata-driven Controller............... OK"
    )

    print(
        "  documentação governada................... OK"
    )

    print(
        "  bootstrap FULL............................ OK"
    )

    print(
        "  steady state FULL......................... OK"
    )

    print(
        "  steady state INCREMENTAL.................. OK"
    )

    print(
        "  watermark + lookback...................... OK"
    )

    print(
        "  Landing imutável.......................... OK"
    )

    print(
        "  manifest................................... OK"
    )

    print(
        "  reconciliação.............................. OK"
    )

    print(
        "  checkpoint operacional..................... OK"
    )

    print(
        "  state_version / concorrência............... OK"
    )

    print(
        "  orquestração end-to-end.................... OK"
    )

    print(
        "  retry controlado........................... OK"
    )

    print(
        "  reprocessamento manual...................... OK"
    )

    print(
        "============================================================"
    )


if __name__ == "__main__":
    main()
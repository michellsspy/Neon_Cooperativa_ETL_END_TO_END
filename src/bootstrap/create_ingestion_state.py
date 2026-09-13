import argparse
import re

from pyspark.sql import SparkSession


# ============================================================
# CONSTANTES
# ============================================================

CONTROL_SCHEMA = "control"

STATE_TABLE = "ingestion_state"


# ============================================================
# ARGUMENTOS
# ============================================================

def parse_args():
    """
    Lê os argumentos utilizados pelo Job.
    """

    parser = argparse.ArgumentParser(
        description=(
            "Cria e inicializa o estado operacional "
            "dos objetos de ingestão."
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

def validate_identifier(value: str) -> str:
    """
    Valida identificadores simples utilizados
    na composição de nomes SQL.
    """

    if not re.fullmatch(
        r"[A-Za-z0-9_]+",
        value,
    ):
        raise ValueError(
            f"Identificador inválido: {value}"
        )

    return value


# ============================================================
# PRÉ-REQUISITOS
# ============================================================

def validate_prerequisites(
    spark: SparkSession,
    catalog: str,
):
    """
    Garante que os metadados declarativos necessários
    já existem antes da criação do estado operacional.
    """

    required_tables = (
        "source_object",
        "ingestion_policy",
    )

    missing_tables = []

    for table_name in required_tables:

        result = spark.sql(
            f"""
            SELECT COUNT(*) AS qtd

            FROM
                `{catalog}`.information_schema.tables

            WHERE
                table_schema = '{CONTROL_SCHEMA}'

                AND table_name = '{table_name}'
            """
        ).first()

        if result["qtd"] == 0:

            missing_tables.append(
                table_name
            )

    if missing_tables:

        raise RuntimeError(
            "Pré-requisitos do Controller ausentes: "
            f"{sorted(missing_tables)}"
        )

    print(
        "Pré-requisitos do Controller validados."
    )


# ============================================================
# CRIAÇÃO DA TABELA
# ============================================================

def create_ingestion_state(
    spark: SparkSession,
    catalog: str,
):
    """
    Cria a tabela responsável exclusivamente
    pelo checkpoint operacional de cada objeto.

    A tabela registra somente estado confirmado.

    Tentativas, erros e execuções em andamento serão
    posteriormente armazenados em ingestion_run e
    ingestion_run_object.
    """

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS
            `{catalog}`.`{CONTROL_SCHEMA}`.`{STATE_TABLE}`
        (

            source_object_key STRING NOT NULL
                COMMENT
                'Identificador técnico do objeto de origem cujo '
                'estado operacional está sendo controlado.',

            bootstrap_completed BOOLEAN NOT NULL
                COMMENT
                'Indica se a carga inicial FULL do objeto foi '
                'concluída e confirmada com sucesso.',

            bootstrap_completed_at TIMESTAMP
                COMMENT
                'Timestamp em que o primeiro bootstrap FULL foi '
                'confirmado com sucesso. Permanece NULL enquanto '
                'bootstrap_completed for false.',

            watermark_column STRING
                COMMENT
                'Coluna de origem à qual o watermark persistido '
                'está vinculado. Para objetos FULL permanece NULL.',

            watermark_data_type STRING
                COMMENT
                'Tipo lógico do watermark persistido. Mantém o '
                'estado explicitamente vinculado ao contrato '
                'incremental utilizado quando foi criado.',

            last_successful_watermark STRING
                COMMENT
                'Último limite superior de watermark confirmado '
                'após materialização e reconciliação bem-sucedidas. '
                'O valor é serializado como texto e interpretado '
                'segundo watermark_data_type.',

            last_successful_run_id STRING
                COMMENT
                'Identificador da última execução de ingestão '
                'confirmada com sucesso para o objeto.',

            last_successful_load_mode STRING
                COMMENT
                'Modo de carga da última execução confirmada com '
                'sucesso, por exemplo FULL ou INCREMENTAL.',

            last_successful_started_at TIMESTAMP
                COMMENT
                'Timestamp de início da última execução de ingestão '
                'confirmada com sucesso.',

            last_successful_completed_at TIMESTAMP
                COMMENT
                'Timestamp de conclusão da última execução de '
                'ingestão confirmada com sucesso.',

            last_successful_row_count BIGINT
                COMMENT
                'Quantidade de registros materializados na Landing '
                'pela última execução confirmada com sucesso.',

            state_version BIGINT NOT NULL
                COMMENT
                'Versão monotônica do estado operacional. Será '
                'incrementada em cada commit de estado bem-sucedido '
                'e permitirá controle otimista de concorrência.',

            created_at TIMESTAMP NOT NULL
                COMMENT
                'Timestamp de criação do estado operacional '
                'do objeto no Controller.',

            updated_at TIMESTAMP NOT NULL
                COMMENT
                'Timestamp da última alteração confirmada '
                'do estado operacional.',

            CONSTRAINT pk_ingestion_state
                PRIMARY KEY (source_object_key)
                NOT ENFORCED
        )

        USING DELTA

        COMMENT
            'Checkpoint operacional da ingestão por objeto. '
            'Contém exclusivamente o último estado confirmado '
            'com sucesso e nunca representa tentativa em andamento.'
        """
    )

    print(
        "Tabela ingestion_state criada/validada."
    )


# ============================================================
# VALIDAÇÃO DAS POLÍTICAS
# ============================================================

def validate_enabled_policies(
    spark: SparkSession,
    catalog: str,
):
    """
    Garante que todo objeto habilitado tenha exatamente
    uma política habilitada antes de receber estado.
    """

    rows = spark.sql(
        f"""
        SELECT
            so.source_object_key,
            so.source_table,
            COUNT(ip.source_object_key) AS policy_count

        FROM
            `{catalog}`.`{CONTROL_SCHEMA}`.`source_object`
            AS so

        LEFT JOIN
            `{catalog}`.`{CONTROL_SCHEMA}`.`ingestion_policy`
            AS ip

            ON
                ip.source_object_key
                =
                so.source_object_key

            AND
                ip.enabled = TRUE

        WHERE
            so.enabled = TRUE

        GROUP BY
            so.source_object_key,
            so.source_table

        ORDER BY
            so.source_table
        """
    ).collect()

    if not rows:

        raise RuntimeError(
            "Nenhum source_object habilitado foi encontrado."
        )

    invalid_objects = [
        (
            row["source_table"],
            row["policy_count"],
        )
        for row in rows
        if row["policy_count"] != 1
    ]

    if invalid_objects:

        details = "; ".join(
            (
                f"{table_name}="
                f"{policy_count} política(s)"
            )
            for table_name, policy_count
            in invalid_objects
        )

        raise RuntimeError(
            "Objetos sem exatamente uma política habilitada: "
            f"{details}"
        )

    print(
        f"Políticas habilitadas validadas: "
        f"{len(rows)} objetos."
    )


# ============================================================
# SEED DO ESTADO
# ============================================================

def seed_ingestion_state(
    spark: SparkSession,
    catalog: str,
):
    """
    Inicializa estado somente para objetos que ainda
    não possuem registro.

    IMPORTANTE:

    Não existe WHEN MATCHED.

    Isso é intencional.

    Reexecutar este bootstrap jamais pode:
      - zerar watermark;
      - zerar state_version;
      - desfazer bootstrap_completed;
      - apagar referência da última execução.

    Estado operacional já existente é preservado integralmente.
    """

    spark.sql(
        f"""
        MERGE INTO
            `{catalog}`.`{CONTROL_SCHEMA}`.`{STATE_TABLE}`
            AS tgt

        USING
        (
            SELECT

                so.source_object_key,

                CAST(FALSE AS BOOLEAN)
                    AS bootstrap_completed,

                CAST(NULL AS TIMESTAMP)
                    AS bootstrap_completed_at,

                CASE

                    WHEN
                        ip.steady_state_mode = 'INCREMENTAL'

                    THEN
                        ip.incremental_column

                    ELSE
                        CAST(NULL AS STRING)

                END
                    AS watermark_column,

                CASE

                    WHEN
                        ip.steady_state_mode = 'INCREMENTAL'

                    THEN
                        ip.watermark_data_type

                    ELSE
                        CAST(NULL AS STRING)

                END
                    AS watermark_data_type,

                CAST(NULL AS STRING)
                    AS last_successful_watermark,

                CAST(NULL AS STRING)
                    AS last_successful_run_id,

                CAST(NULL AS STRING)
                    AS last_successful_load_mode,

                CAST(NULL AS TIMESTAMP)
                    AS last_successful_started_at,

                CAST(NULL AS TIMESTAMP)
                    AS last_successful_completed_at,

                CAST(NULL AS BIGINT)
                    AS last_successful_row_count,

                CAST(0 AS BIGINT)
                    AS state_version,

                current_timestamp()
                    AS created_at,

                current_timestamp()
                    AS updated_at

            FROM
                `{catalog}`.`{CONTROL_SCHEMA}`.`source_object`
                AS so

            INNER JOIN
                `{catalog}`.`{CONTROL_SCHEMA}`.`ingestion_policy`
                AS ip

                ON
                    ip.source_object_key
                    =
                    so.source_object_key

            WHERE
                so.enabled = TRUE

                AND ip.enabled = TRUE

        ) AS src

        ON
            tgt.source_object_key
            =
            src.source_object_key

        WHEN NOT MATCHED THEN

            INSERT
            (
                source_object_key,

                bootstrap_completed,
                bootstrap_completed_at,

                watermark_column,
                watermark_data_type,

                last_successful_watermark,

                last_successful_run_id,
                last_successful_load_mode,

                last_successful_started_at,
                last_successful_completed_at,

                last_successful_row_count,

                state_version,

                created_at,
                updated_at
            )

            VALUES
            (
                src.source_object_key,

                src.bootstrap_completed,
                src.bootstrap_completed_at,

                src.watermark_column,
                src.watermark_data_type,

                src.last_successful_watermark,

                src.last_successful_run_id,
                src.last_successful_load_mode,

                src.last_successful_started_at,
                src.last_successful_completed_at,

                src.last_successful_row_count,

                src.state_version,

                src.created_at,
                src.updated_at
            )
        """
    )

    print(
        "Seed idempotente de ingestion_state concluído."
    )


# ============================================================
# VALIDAÇÃO DA UNICIDADE
# ============================================================

def validate_state_uniqueness(
    spark: SparkSession,
    catalog: str,
):
    """
    A PRIMARY KEY do Databricks é informativa.

    Portanto, a própria plataforma valida que não existem
    múltiplos estados para o mesmo source_object_key.
    """

    duplicate_count = spark.sql(
        f"""
        SELECT
            COUNT(*) AS qtd

        FROM
        (
            SELECT
                source_object_key

            FROM
                `{catalog}`.`{CONTROL_SCHEMA}`.`{STATE_TABLE}`

            GROUP BY
                source_object_key

            HAVING
                COUNT(*) > 1
        )
        """
    ).first()["qtd"]

    if duplicate_count != 0:

        raise RuntimeError(
            "Foram encontrados source_object_key duplicados "
            "em ingestion_state."
        )

    print(
        "Unicidade lógica de ingestion_state validada."
    )


# ============================================================
# VALIDAÇÃO DA COBERTURA
# ============================================================

def validate_state_coverage(
    spark: SparkSession,
    catalog: str,
):
    """
    Garante que todo objeto habilitado com política
    habilitada possua exatamente um estado operacional.
    """

    expected_count = spark.sql(
        f"""
        SELECT
            COUNT(*) AS qtd

        FROM
            `{catalog}`.`{CONTROL_SCHEMA}`.`source_object`
            AS so

        INNER JOIN
            `{catalog}`.`{CONTROL_SCHEMA}`.`ingestion_policy`
            AS ip

            ON
                ip.source_object_key
                =
                so.source_object_key

        WHERE
            so.enabled = TRUE

            AND ip.enabled = TRUE
        """
    ).first()["qtd"]

    actual_count = spark.sql(
        f"""
        SELECT
            COUNT(*) AS qtd

        FROM
            `{catalog}`.`{CONTROL_SCHEMA}`.`{STATE_TABLE}`
            AS st

        INNER JOIN
            `{catalog}`.`{CONTROL_SCHEMA}`.`source_object`
            AS so

            ON
                so.source_object_key
                =
                st.source_object_key

        INNER JOIN
            `{catalog}`.`{CONTROL_SCHEMA}`.`ingestion_policy`
            AS ip

            ON
                ip.source_object_key
                =
                st.source_object_key

        WHERE
            so.enabled = TRUE

            AND ip.enabled = TRUE
        """
    ).first()["qtd"]

    if actual_count != expected_count:

        raise RuntimeError(
            "Cobertura inválida de ingestion_state. "
            f"Esperado={expected_count}, "
            f"encontrado={actual_count}"
        )

    print(
        f"Cobertura de ingestion_state validada: "
        f"{actual_count} objetos."
    )


# ============================================================
# VALIDAÇÃO DO BINDING WATERMARK ↔ POLICY
# ============================================================

def validate_watermark_binding(
    spark: SparkSession,
    catalog: str,
):
    """
    Detecta inconsistência entre a política atual
    e o contrato de watermark registrado no estado.

    Essa validação é crítica porque alterar a coluna
    incremental sem reinicializar conscientemente o
    estado poderia reutilizar um watermark incompatível.
    """

    invalid_rows = spark.sql(
        f"""
        SELECT

            so.source_table,

            ip.steady_state_mode,

            ip.incremental_column,
            ip.watermark_data_type,

            st.watermark_column,
            st.watermark_data_type
                AS state_watermark_data_type

        FROM
            `{catalog}`.`{CONTROL_SCHEMA}`.`{STATE_TABLE}`
            AS st

        INNER JOIN
            `{catalog}`.`{CONTROL_SCHEMA}`.`source_object`
            AS so

            ON
                so.source_object_key
                =
                st.source_object_key

        INNER JOIN
            `{catalog}`.`{CONTROL_SCHEMA}`.`ingestion_policy`
            AS ip

            ON
                ip.source_object_key
                =
                st.source_object_key

        WHERE
            so.enabled = TRUE

            AND ip.enabled = TRUE

            AND
            (
                (
                    ip.steady_state_mode = 'INCREMENTAL'

                    AND
                    (
                        st.watermark_column
                            IS DISTINCT FROM
                        ip.incremental_column

                        OR

                        st.watermark_data_type
                            IS DISTINCT FROM
                        ip.watermark_data_type
                    )
                )

                OR

                (
                    ip.steady_state_mode = 'FULL'

                    AND
                    (
                        st.watermark_column IS NOT NULL

                        OR

                        st.watermark_data_type IS NOT NULL
                    )
                )
            )
        """
    ).collect()

    if invalid_rows:

        details = "; ".join(
            (
                f"{row['source_table']}: "
                f"policy={row['incremental_column']}/"
                f"{row['watermark_data_type']} "
                f"state={row['watermark_column']}/"
                f"{row['state_watermark_data_type']}"
            )
            for row
            in invalid_rows
        )

        raise RuntimeError(
            "Estado de watermark incompatível com "
            "a política atual: "
            f"{details}"
        )

    print(
        "Binding entre ingestion_policy e "
        "ingestion_state validado."
    )


# ============================================================
# RESUMO FINAL
# ============================================================

def print_state_summary(
    spark: SparkSession,
    catalog: str,
):
    """
    Apresenta o estado consolidado após o bootstrap.
    """

    summary = spark.sql(
        f"""
        SELECT

            COUNT(*) AS total,

            SUM(
                CASE
                    WHEN bootstrap_completed
                    THEN 1
                    ELSE 0
                END
            ) AS bootstrap_completed,

            SUM(
                CASE
                    WHEN NOT bootstrap_completed
                    THEN 1
                    ELSE 0
                END
            ) AS bootstrap_pending,

            SUM(
                CASE
                    WHEN last_successful_watermark IS NOT NULL
                    THEN 1
                    ELSE 0
                END
            ) AS watermark_initialized

        FROM
            `{catalog}`.`{CONTROL_SCHEMA}`.`{STATE_TABLE}`
        """
    ).first()

    print(
        "============================================================"
    )

    print(
        "Resumo de ingestion_state:"
    )

    print(
        f"  total....................: "
        f"{summary['total']}"
    )

    print(
        f"  bootstrap concluído......: "
        f"{summary['bootstrap_completed']}"
    )

    print(
        f"  bootstrap pendente.......: "
        f"{summary['bootstrap_pending']}"
    )

    print(
        f"  watermark inicializado...: "
        f"{summary['watermark_initialized']}"
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

    spark = (
        SparkSession
        .builder
        .getOrCreate()
    )

    print(
        "============================================================"
    )

    print(
        "Bootstrap do estado operacional de ingestão"
    )

    print(
        f"Controller: "
        f"{catalog}.{CONTROL_SCHEMA}"
    )

    print(
        "============================================================"
    )

    # --------------------------------------------------------
    # 1. Pré-requisitos
    # --------------------------------------------------------

    validate_prerequisites(
        spark=spark,
        catalog=catalog,
    )

    # --------------------------------------------------------
    # 2. Criação da estrutura
    # --------------------------------------------------------

    create_ingestion_state(
        spark=spark,
        catalog=catalog,
    )

    # --------------------------------------------------------
    # 3. Validação das políticas
    # --------------------------------------------------------

    validate_enabled_policies(
        spark=spark,
        catalog=catalog,
    )

    # --------------------------------------------------------
    # 4. Seed inicial
    # --------------------------------------------------------

    seed_ingestion_state(
        spark=spark,
        catalog=catalog,
    )

    # --------------------------------------------------------
    # 5. Validações
    # --------------------------------------------------------

    validate_state_uniqueness(
        spark=spark,
        catalog=catalog,
    )

    validate_state_coverage(
        spark=spark,
        catalog=catalog,
    )

    validate_watermark_binding(
        spark=spark,
        catalog=catalog,
    )

    # --------------------------------------------------------
    # 6. Resumo
    # --------------------------------------------------------

    print_state_summary(
        spark=spark,
        catalog=catalog,
    )

    print(
        "Bootstrap de ingestion_state "
        "concluído com sucesso."
    )


if __name__ == "__main__":
    main()
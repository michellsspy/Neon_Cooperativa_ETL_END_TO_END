import argparse
import re
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


# ============================================================
# CONSTANTES
# ============================================================

CONTROL_SCHEMA = "control"
LANDING_SCHEMA = "landing"

SOURCE_TYPE = "POSTGRESQL"
SOURCE_KEY_ORIGIN = "CONFIGURED"


# ============================================================
# OBJETOS DE ORIGEM DO LABORATÓRIO
# ============================================================

SOURCE_OBJECTS = {
    "agencias": {
        "key_columns": ["agencia_id"],
        "description": "Cadastro das agências da cooperativa.",
    },
    "atendimentos_relacionamento": {
        "key_columns": ["atendimento_id"],
        "description": (
            "Registros de atendimentos e relacionamento "
            "com os cooperados."
        ),
    },
    "contas_correntes": {
        "key_columns": ["conta_id"],
        "description": (
            "Cadastro das contas correntes mantidas pelos cooperados."
        ),
    },
    "contratos_credito": {
        "key_columns": ["contrato_credito_id"],
        "description": (
            "Contratos de crédito firmados pelos cooperados."
        ),
    },
    "cooperados": {
        "key_columns": ["cooperado_id"],
        "description": "Cadastro dos cooperados da cooperativa.",
    },
    "investimentos": {
        "key_columns": ["investimento_id"],
        "description": "Investimentos mantidos pelos cooperados.",
    },
    "lancamentos_contabeis": {
        "key_columns": ["lancamento_id"],
        "description": (
            "Lançamentos contábeis registrados pela cooperativa."
        ),
    },
    "parcelas_credito": {
        "key_columns": ["parcela_credito_id"],
        "description": (
            "Parcelas associadas aos contratos de crédito."
        ),
    },
    "produtos_credito": {
        "key_columns": ["produto_credito_id"],
        "description": (
            "Cadastro dos produtos de crédito oferecidos "
            "pela cooperativa."
        ),
    },
    "score_risco_cooperado": {
        "key_columns": ["score_id"],
        "description": (
            "Registros de avaliação de risco dos cooperados."
        ),
    },
}


# ============================================================
# ARGUMENTOS
# ============================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Realiza discovery e seed governado dos metadados "
            "da origem Neon PostgreSQL."
        )
    )

    parser.add_argument(
        "--catalog",
        required=True,
        help="Catálogo gerenciado do projeto.",
    )

    parser.add_argument(
        "--source-catalog",
        required=True,
        help="Foreign Catalog que representa a origem.",
    )

    parser.add_argument(
        "--source-schema",
        required=True,
        help="Schema da origem PostgreSQL.",
    )

    parser.add_argument(
        "--source-system-key",
        required=True,
        help="Identificador lógico do sistema de origem.",
    )

    parser.add_argument(
        "--source-connection",
        required=True,
        help="Nome da conexão Unity Catalog usada pelo Foreign Catalog.",
    )

    parser.add_argument(
        "--landing-volume",
        required=True,
        help="Volume utilizado pela Landing Zone.",
    )

    return parser.parse_args()


# ============================================================
# UTILITÁRIOS
# ============================================================

def validate_identifier(value: str) -> str:
    """
    Valida identificadores utilizados na composição
    de nomes de objetos SQL.
    """

    if not re.fullmatch(r"[A-Za-z0-9_]+", value):
        raise ValueError(
            f"Identificador inválido: {value}"
        )

    return value


def escape_sql_literal(value: str) -> str:
    """
    Escapa aspas simples utilizadas em literais SQL.
    """

    return value.replace("'", "''")


def utc_now() -> datetime:
    """
    Retorna timestamp UTC sem timezone associado,
    compatível com Spark TIMESTAMP.
    """

    return (
        datetime
        .now(timezone.utc)
        .replace(tzinfo=None)
    )


def table_exists(
    spark: SparkSession,
    catalog: str,
    schema: str,
    table: str,
) -> bool:
    """
    Verifica existência de tabela no Unity Catalog.
    """

    schema_literal = escape_sql_literal(schema)
    table_literal = escape_sql_literal(table)

    result = spark.sql(
        f"""
        SELECT COUNT(*) AS qtd
        FROM `{catalog}`.information_schema.tables
        WHERE table_schema = '{schema_literal}'
          AND table_name = '{table_literal}'
        """
    ).first()

    return result["qtd"] > 0


# ============================================================
# VALIDAÇÃO DO CONTROLLER
# ============================================================

def assert_controller_prerequisites(
    spark: SparkSession,
    catalog: str,
):
    """
    Confirma que o bootstrap do Controller já foi executado.
    """

    required_tables = (
        "source_system",
        "source_object",
        "ingestion_policy",
    )

    missing_tables = []

    for table_name in required_tables:

        exists = table_exists(
            spark=spark,
            catalog=catalog,
            schema=CONTROL_SCHEMA,
            table=table_name,
        )

        if not exists:
            missing_tables.append(table_name)

    if missing_tables:
        raise RuntimeError(
            "Controller core incompleto. "
            f"Tabelas ausentes: {sorted(missing_tables)}"
        )

    print(
        "Controller core validado."
    )


# ============================================================
# SOURCE_COLUMN
# ============================================================

def create_source_column(
    spark: SparkSession,
    catalog: str,
):
    """
    Cria o catálogo técnico das colunas da origem.
    """

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS
            `{catalog}`.`{CONTROL_SCHEMA}`.`source_column`
        (
            source_column_key STRING NOT NULL,

            source_object_key STRING NOT NULL,

            column_name STRING NOT NULL,

            ordinal_position INT NOT NULL,

            source_data_type STRING NOT NULL,

            is_nullable BOOLEAN NOT NULL,

            is_source_key BOOLEAN NOT NULL,

            source_comment STRING,

            active BOOLEAN NOT NULL,

            discovered_at TIMESTAMP NOT NULL,

            updated_at TIMESTAMP NOT NULL,

            CONSTRAINT pk_source_column
                PRIMARY KEY (source_column_key) NOT ENFORCED
        )
        USING DELTA

        COMMENT
            'Catálogo técnico das colunas descobertas nos objetos '
            'de origem, contendo posição, tipo, nulabilidade, '
            'participação na chave lógica, comentário de origem '
            'quando disponível e estado atual.'
        """
    )


# ============================================================
# DOCUMENTAÇÃO DE SOURCE_COLUMN
# ============================================================

def apply_source_column_comments(
    spark: SparkSession,
    catalog: str,
):
    """
    Garante documentação das colunas do catálogo técnico.
    """

    table_fqn = (
        f"`{catalog}`."
        f"`{CONTROL_SCHEMA}`."
        f"`source_column`"
    )

    column_comments = {
        "source_column_key": (
            "Identificador técnico e determinístico da coluna, "
            "formado pelo identificador do objeto de origem "
            "e pelo nome da coluna."
        ),
        "source_object_key": (
            "Identificador do objeto de origem ao qual "
            "a coluna pertence."
        ),
        "column_name": (
            "Nome da coluna conforme exposto pelo sistema de origem."
        ),
        "ordinal_position": (
            "Posição ordinal da coluna dentro do objeto de origem, "
            "iniciando em 1."
        ),
        "source_data_type": (
            "Tipo de dado reportado pelo catálogo técnico da origem."
        ),
        "is_nullable": (
            "Indica se a coluna permite valores nulos segundo "
            "o metadado atualmente exposto pela origem."
        ),
        "is_source_key": (
            "Indica se a coluna participa da chave lógica "
            "configurada pela plataforma para identificar "
            "registros do objeto."
        ),
        "source_comment": (
            "Comentário proveniente do sistema de origem, quando "
            "o conector e o Foreign Catalog disponibilizam esse "
            "metadado. NULL significa que o comentário não foi "
            "exposto pela origem ou pela federação."
        ),
        "active": (
            "Indica se a coluna foi encontrada no snapshot "
            "estrutural mais recente da origem."
        ),
        "discovered_at": (
            "Timestamp UTC em que a coluna foi inicialmente "
            "registrada pelo processo de discovery."
        ),
        "updated_at": (
            "Timestamp UTC da última atualização dos metadados "
            "registrados para a coluna."
        ),
    }

    for column_name, comment in column_comments.items():

        escaped_comment = escape_sql_literal(
            comment
        )

        spark.sql(
            f"""
            COMMENT ON COLUMN
                {table_fqn}.`{column_name}`
            IS
                '{escaped_comment}'
            """
        )


# ============================================================
# DISCOVERY DAS TABELAS
# ============================================================

def discover_source_tables(
    spark: SparkSession,
    source_catalog: str,
    source_schema: str,
) -> set[str]:
    """
    Descobre as tabelas físicas disponíveis na origem.
    """

    source_schema_literal = escape_sql_literal(
        source_schema
    )

    rows = spark.sql(
        f"""
        SELECT
            table_name
        FROM
            `{source_catalog}`.information_schema.tables
        WHERE
            table_schema = '{source_schema_literal}'
            AND table_type = 'BASE TABLE'
        ORDER BY
            table_name
        """
    ).collect()

    return {
        row["table_name"]
        for row in rows
    }


def validate_configured_objects(
    discovered_tables: set[str],
):
    """
    Verifica se todos os objetos configurados existem
    atualmente na origem.
    """

    configured_tables = set(
        SOURCE_OBJECTS.keys()
    )

    missing_tables = (
        configured_tables
        - discovered_tables
    )

    if missing_tables:
        raise RuntimeError(
            "Objetos configurados não encontrados na origem: "
            f"{sorted(missing_tables)}"
        )

    extra_tables = (
        discovered_tables
        - configured_tables
    )

    if extra_tables:

        print(
            "ATENÇÃO: existem tabelas adicionais na origem "
            "que ainda não estão habilitadas no Controller:"
        )

        for table_name in sorted(extra_tables):
            print(
                f"  - {table_name}"
            )


# ============================================================
# SEED SOURCE_SYSTEM
# ============================================================

def seed_source_system(
    spark: SparkSession,
    catalog: str,
    source_catalog: str,
    source_system_key: str,
    source_connection: str,
):
    """
    Insere ou atualiza o sistema de origem.
    """

    now = utc_now()

    dataframe_schema = StructType(
        [
            StructField(
                "source_system_key",
                StringType(),
                False,
            ),
            StructField(
                "source_type",
                StringType(),
                False,
            ),
            StructField(
                "connection_name",
                StringType(),
                True,
            ),
            StructField(
                "foreign_catalog",
                StringType(),
                True,
            ),
            StructField(
                "description",
                StringType(),
                True,
            ),
            StructField(
                "owner",
                StringType(),
                True,
            ),
            StructField(
                "enabled",
                BooleanType(),
                False,
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

    rows = [
        (
            source_system_key,
            SOURCE_TYPE,
            source_connection,
            source_catalog,
            (
                "Banco PostgreSQL Neon utilizado como origem "
                "do laboratório Neon Cooperativa."
            ),
            None,
            True,
            now,
            now,
        )
    ]

    df = spark.createDataFrame(
        rows,
        schema=dataframe_schema,
    )

    df.createOrReplaceTempView(
        "seed_source_system"
    )

    spark.sql(
        f"""
        MERGE INTO
            `{catalog}`.`{CONTROL_SCHEMA}`.`source_system` AS tgt

        USING
            seed_source_system AS src

        ON
            tgt.source_system_key
            =
            src.source_system_key

        WHEN MATCHED THEN
            UPDATE SET

                tgt.source_type =
                    src.source_type,

                tgt.connection_name =
                    src.connection_name,

                tgt.foreign_catalog =
                    src.foreign_catalog,

                tgt.description =
                    src.description,

                tgt.enabled =
                    src.enabled,

                tgt.updated_at =
                    src.updated_at

        WHEN NOT MATCHED THEN
            INSERT (
                source_system_key,
                source_type,
                connection_name,
                foreign_catalog,
                description,
                owner,
                enabled,
                created_at,
                updated_at
            )
            VALUES (
                src.source_system_key,
                src.source_type,
                src.connection_name,
                src.foreign_catalog,
                src.description,
                src.owner,
                src.enabled,
                src.created_at,
                src.updated_at
            )
        """
    )

    print(
        "source_system atualizado."
    )


# ============================================================
# SEED SOURCE_OBJECT
# ============================================================

def seed_source_objects(
    spark: SparkSession,
    catalog: str,
    source_catalog: str,
    source_schema: str,
    source_system_key: str,
    landing_volume: str,
):
    """
    Insere ou atualiza os objetos configurados da origem.
    """

    now = utc_now()

    dataframe_schema = StructType(
        [
            StructField(
                "source_object_key",
                StringType(),
                False,
            ),
            StructField(
                "source_system_key",
                StringType(),
                False,
            ),
            StructField(
                "source_schema",
                StringType(),
                False,
            ),
            StructField(
                "source_table",
                StringType(),
                False,
            ),
            StructField(
                "source_fqn",
                StringType(),
                False,
            ),
            StructField(
                "source_key_columns",
                ArrayType(
                    StringType(),
                    containsNull=False,
                ),
                True,
            ),
            StructField(
                "source_key_origin",
                StringType(),
                False,
            ),
            StructField(
                "landing_catalog",
                StringType(),
                False,
            ),
            StructField(
                "landing_schema",
                StringType(),
                False,
            ),
            StructField(
                "landing_volume",
                StringType(),
                False,
            ),
            StructField(
                "landing_relative_path",
                StringType(),
                False,
            ),
            StructField(
                "description",
                StringType(),
                True,
            ),
            StructField(
                "enabled",
                BooleanType(),
                False,
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

    rows = []

    for table_name, config in sorted(
        SOURCE_OBJECTS.items()
    ):

        source_object_key = (
            f"{source_system_key}."
            f"{source_schema}."
            f"{table_name}"
        )

        source_fqn = (
            f"{source_catalog}."
            f"{source_schema}."
            f"{table_name}"
        )

        landing_relative_path = (
            f"postgresql/"
            f"{source_catalog}/"
            f"{source_schema}/"
            f"{table_name}"
        )

        rows.append(
            (
                source_object_key,
                source_system_key,
                source_schema,
                table_name,
                source_fqn,
                config["key_columns"],
                SOURCE_KEY_ORIGIN,
                catalog,
                LANDING_SCHEMA,
                landing_volume,
                landing_relative_path,
                config["description"],
                True,
                now,
                now,
            )
        )

    df = spark.createDataFrame(
        rows,
        schema=dataframe_schema,
    )

    df.createOrReplaceTempView(
        "seed_source_object"
    )

    spark.sql(
        f"""
        MERGE INTO
            `{catalog}`.`{CONTROL_SCHEMA}`.`source_object` AS tgt

        USING
            seed_source_object AS src

        ON
            tgt.source_object_key
            =
            src.source_object_key

        WHEN MATCHED THEN
            UPDATE SET

                tgt.source_system_key =
                    src.source_system_key,

                tgt.source_schema =
                    src.source_schema,

                tgt.source_table =
                    src.source_table,

                tgt.source_fqn =
                    src.source_fqn,

                tgt.source_key_columns =
                    src.source_key_columns,

                tgt.source_key_origin =
                    src.source_key_origin,

                tgt.landing_catalog =
                    src.landing_catalog,

                tgt.landing_schema =
                    src.landing_schema,

                tgt.landing_volume =
                    src.landing_volume,

                tgt.landing_relative_path =
                    src.landing_relative_path,

                tgt.description =
                    src.description,

                tgt.enabled =
                    src.enabled,

                tgt.updated_at =
                    src.updated_at

        WHEN NOT MATCHED THEN
            INSERT (
                source_object_key,
                source_system_key,
                source_schema,
                source_table,
                source_fqn,
                source_key_columns,
                source_key_origin,
                landing_catalog,
                landing_schema,
                landing_volume,
                landing_relative_path,
                description,
                enabled,
                created_at,
                updated_at
            )
            VALUES (
                src.source_object_key,
                src.source_system_key,
                src.source_schema,
                src.source_table,
                src.source_fqn,
                src.source_key_columns,
                src.source_key_origin,
                src.landing_catalog,
                src.landing_schema,
                src.landing_volume,
                src.landing_relative_path,
                src.description,
                src.enabled,
                src.created_at,
                src.updated_at
            )
        """
    )

    print(
        f"source_object atualizado com "
        f"{len(rows)} objetos configurados."
    )


# ============================================================
# DISCOVERY DO CONTRATO DO INFORMATION_SCHEMA
# ============================================================

def get_source_columns_metadata_contract(
    spark: SparkSession,
    source_catalog: str,
) -> set[str]:
    """
    Descobre dinamicamente quais campos são realmente
    expostos pelo information_schema.columns do Foreign Catalog.

    Isso evita assumir que todos os metadados opcionais
    suportados pelo Unity Catalog nativo estejam presentes
    na implementação federada da origem.
    """

    metadata_df = spark.sql(
        f"""
        SELECT *
        FROM `{source_catalog}`.information_schema.columns
        LIMIT 0
        """
    )

    available_columns = {
        column_name.lower()
        for column_name in metadata_df.columns
    }

    print(
        "Metadados disponíveis em "
        f"{source_catalog}.information_schema.columns:"
    )

    for column_name in sorted(
        available_columns
    ):
        print(
            f"  - {column_name}"
        )

    return available_columns


# ============================================================
# DISCOVERY DAS COLUNAS DA ORIGEM
# ============================================================

def discover_source_columns(
    spark: SparkSession,
    source_catalog: str,
    source_schema: str,
):
    """
    Descobre as colunas das tabelas configuradas.

    O campo COMMENT é tratado como opcional porque Foreign
    Catalogs podem expor um subconjunto diferente dos metadados
    disponíveis no information_schema nativo do Unity Catalog.
    """

    configured_tables = sorted(
        SOURCE_OBJECTS.keys()
    )

    table_filter = ", ".join(
        f"'{escape_sql_literal(table_name)}'"
        for table_name in configured_tables
    )

    available_metadata_columns = (
        get_source_columns_metadata_contract(
            spark=spark,
            source_catalog=source_catalog,
        )
    )

    if "comment" in available_metadata_columns:

        source_comment_expression = (
            "comment AS source_comment"
        )

        print(
            "Metadado COMMENT disponível no Foreign Catalog."
        )

    else:

        source_comment_expression = (
            "CAST(NULL AS STRING) AS source_comment"
        )

        print(
            "Metadado COMMENT não é exposto pelo "
            "information_schema.columns deste Foreign Catalog. "
            "source_comment será registrado como NULL."
        )

    source_schema_literal = escape_sql_literal(
        source_schema
    )

    rows = spark.sql(
        f"""
        SELECT

            table_name,

            column_name,

            ordinal_position,

            data_type,

            is_nullable,

            {source_comment_expression}

        FROM
            `{source_catalog}`.information_schema.columns

        WHERE
            table_schema = '{source_schema_literal}'

            AND table_name IN (
                {table_filter}
            )

        ORDER BY
            table_name,
            ordinal_position
        """
    ).collect()

    if not rows:
        raise RuntimeError(
            "Nenhuma coluna foi descoberta na origem."
        )

    discovered_tables = {
        row["table_name"]
        for row in rows
    }

    missing_tables = (
        set(SOURCE_OBJECTS.keys())
        - discovered_tables
    )

    if missing_tables:
        raise RuntimeError(
            "Metadados de coluna não encontrados para: "
            f"{sorted(missing_tables)}"
        )

    return rows


# ============================================================
# VALIDAÇÃO DAS CHAVES CONFIGURADAS
# ============================================================

def validate_source_key_columns(
    discovered_rows,
):
    """
    Garante que toda chave lógica configurada realmente
    exista no schema atual da origem.
    """

    columns_by_table = {}

    for row in discovered_rows:

        table_name = row["table_name"]
        column_name = row["column_name"]

        columns_by_table.setdefault(
            table_name,
            set(),
        )

        columns_by_table[
            table_name
        ].add(
            column_name
        )

    errors = []

    for table_name, config in SOURCE_OBJECTS.items():

        discovered_columns = (
            columns_by_table.get(
                table_name,
                set(),
            )
        )

        configured_key_columns = set(
            config["key_columns"]
        )

        missing_key_columns = (
            configured_key_columns
            - discovered_columns
        )

        if missing_key_columns:

            errors.append(
                (
                    table_name,
                    sorted(
                        missing_key_columns
                    ),
                )
            )

    if errors:

        error_message = "; ".join(
            (
                f"{table_name}: "
                f"{missing_columns}"
            )
            for table_name, missing_columns
            in errors
        )

        raise RuntimeError(
            "Chaves lógicas configuradas não encontradas "
            f"na origem: {error_message}"
        )

    print(
        "Chaves lógicas configuradas validadas contra "
        "o schema atual da origem."
    )


# ============================================================
# CONVERSÃO DE NULLABILITY
# ============================================================

def parse_is_nullable(value) -> bool:
    """
    Normaliza a representação de nulabilidade recebida
    pelo information_schema.
    """

    if isinstance(value, bool):
        return value

    normalized_value = (
        str(value)
        .strip()
        .upper()
    )

    if normalized_value == "YES":
        return True

    if normalized_value == "NO":
        return False

    raise ValueError(
        "Valor inesperado em is_nullable: "
        f"{value}"
    )


# ============================================================
# SEED SOURCE_COLUMN
# ============================================================

def seed_source_columns(
    spark: SparkSession,
    catalog: str,
    source_schema: str,
    source_system_key: str,
    discovered_rows,
):
    """
    Persiste o snapshot estrutural atual das colunas.

    O MERGE é idempotente e também marca como inactive
    colunas previamente conhecidas que desapareceram
    do snapshot atual.
    """

    now = utc_now()

    dataframe_schema = StructType(
        [
            StructField(
                "source_column_key",
                StringType(),
                False,
            ),
            StructField(
                "source_object_key",
                StringType(),
                False,
            ),
            StructField(
                "column_name",
                StringType(),
                False,
            ),
            StructField(
                "ordinal_position",
                IntegerType(),
                False,
            ),
            StructField(
                "source_data_type",
                StringType(),
                False,
            ),
            StructField(
                "is_nullable",
                BooleanType(),
                False,
            ),
            StructField(
                "is_source_key",
                BooleanType(),
                False,
            ),
            StructField(
                "source_comment",
                StringType(),
                True,
            ),
            StructField(
                "active",
                BooleanType(),
                False,
            ),
            StructField(
                "discovered_at",
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

    rows = []

    for source_row in discovered_rows:

        table_name = (
            source_row["table_name"]
        )

        column_name = (
            source_row["column_name"]
        )

        source_object_key = (
            f"{source_system_key}."
            f"{source_schema}."
            f"{table_name}"
        )

        source_column_key = (
            f"{source_object_key}."
            f"{column_name}"
        )

        configured_key_columns = (
            SOURCE_OBJECTS[
                table_name
            ]["key_columns"]
        )

        is_source_key = (
            column_name
            in configured_key_columns
        )

        is_nullable = parse_is_nullable(
            source_row["is_nullable"]
        )

        rows.append(
            (
                source_column_key,
                source_object_key,
                column_name,
                int(
                    source_row[
                        "ordinal_position"
                    ]
                ),
                str(
                    source_row[
                        "data_type"
                    ]
                ),
                is_nullable,
                is_source_key,
                source_row[
                    "source_comment"
                ],
                True,
                now,
                now,
            )
        )

    df = spark.createDataFrame(
        rows,
        schema=dataframe_schema,
    )

    df.createOrReplaceTempView(
        "seed_source_column"
    )

    configured_object_keys = [
        (
            f"{source_system_key}."
            f"{source_schema}."
            f"{table_name}"
        )
        for table_name
        in SOURCE_OBJECTS.keys()
    ]

    object_filter = ", ".join(
        f"'{escape_sql_literal(key)}'"
        for key in configured_object_keys
    )

    spark.sql(
        f"""
        MERGE INTO
            `{catalog}`.`{CONTROL_SCHEMA}`.`source_column`
            AS tgt

        USING
            seed_source_column
            AS src

        ON
            tgt.source_column_key
            =
            src.source_column_key

        WHEN MATCHED THEN
            UPDATE SET

                tgt.source_object_key =
                    src.source_object_key,

                tgt.column_name =
                    src.column_name,

                tgt.ordinal_position =
                    src.ordinal_position,

                tgt.source_data_type =
                    src.source_data_type,

                tgt.is_nullable =
                    src.is_nullable,

                tgt.is_source_key =
                    src.is_source_key,

                tgt.source_comment =
                    src.source_comment,

                tgt.active =
                    TRUE,

                tgt.updated_at =
                    src.updated_at

        WHEN NOT MATCHED THEN
            INSERT (
                source_column_key,
                source_object_key,
                column_name,
                ordinal_position,
                source_data_type,
                is_nullable,
                is_source_key,
                source_comment,
                active,
                discovered_at,
                updated_at
            )
            VALUES (
                src.source_column_key,
                src.source_object_key,
                src.column_name,
                src.ordinal_position,
                src.source_data_type,
                src.is_nullable,
                src.is_source_key,
                src.source_comment,
                src.active,
                src.discovered_at,
                src.updated_at
            )

        WHEN NOT MATCHED BY SOURCE
             AND tgt.source_object_key IN (
                 {object_filter}
             )
        THEN
            UPDATE SET

                tgt.active =
                    FALSE,

                tgt.updated_at =
                    current_timestamp()
        """
    )

    print(
        f"source_column atualizado com "
        f"{len(rows)} colunas descobertas."
    )


# ============================================================
# VALIDAÇÃO FINAL
# ============================================================

def validate_seed(
    spark: SparkSession,
    catalog: str,
    source_system_key: str,
):
    """
    Valida o estado final dos metadados persistidos.
    """

    source_system_key_literal = (
        escape_sql_literal(
            source_system_key
        )
    )

    system_count = spark.sql(
        f"""
        SELECT COUNT(*) AS qtd
        FROM
            `{catalog}`.`{CONTROL_SCHEMA}`.`source_system`
        WHERE
            source_system_key =
            '{source_system_key_literal}'
        """
    ).first()["qtd"]

    object_count = spark.sql(
        f"""
        SELECT COUNT(*) AS qtd
        FROM
            `{catalog}`.`{CONTROL_SCHEMA}`.`source_object`
        WHERE
            source_system_key =
            '{source_system_key_literal}'
            AND enabled = TRUE
        """
    ).first()["qtd"]

    active_column_count = spark.sql(
        f"""
        SELECT COUNT(*) AS qtd
        FROM
            `{catalog}`.`{CONTROL_SCHEMA}`.`source_column`
        WHERE
            active = TRUE
            AND source_object_key LIKE
                '{source_system_key_literal}.%'
        """
    ).first()["qtd"]

    source_key_column_count = spark.sql(
        f"""
        SELECT COUNT(*) AS qtd
        FROM
            `{catalog}`.`{CONTROL_SCHEMA}`.`source_column`
        WHERE
            active = TRUE
            AND is_source_key = TRUE
            AND source_object_key LIKE
                '{source_system_key_literal}.%'
        """
    ).first()["qtd"]

    if system_count != 1:
        raise RuntimeError(
            "Quantidade inesperada de source_system. "
            f"Esperado=1, encontrado={system_count}"
        )

    if object_count != len(
        SOURCE_OBJECTS
    ):
        raise RuntimeError(
            "Quantidade inesperada de source_object habilitados. "
            f"Esperado={len(SOURCE_OBJECTS)}, "
            f"encontrado={object_count}"
        )

    if active_column_count <= 0:
        raise RuntimeError(
            "Nenhuma coluna ativa foi registrada."
        )

    if source_key_column_count != len(
        SOURCE_OBJECTS
    ):
        raise RuntimeError(
            "Quantidade inesperada de colunas identificadas "
            "como chave lógica. "
            f"Esperado={len(SOURCE_OBJECTS)}, "
            f"encontrado={source_key_column_count}"
        )

    print(
        "Validação final do seed:"
    )

    print(
        f"  source_system........: {system_count}"
    )

    print(
        f"  source_object........: {object_count}"
    )

    print(
        f"  source_column ativas.: {active_column_count}"
    )

    print(
        f"  colunas chave........: {source_key_column_count}"
    )


# ============================================================
# MAIN
# ============================================================

def main():

    args = parse_args()

    catalog = validate_identifier(
        args.catalog
    )

    source_catalog = validate_identifier(
        args.source_catalog
    )

    source_schema = validate_identifier(
        args.source_schema
    )

    source_system_key = validate_identifier(
        args.source_system_key
    )

    source_connection = validate_identifier(
        args.source_connection
    )

    landing_volume = validate_identifier(
        args.landing_volume
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
        "Iniciando seed governado dos metadados de origem"
    )

    print(
        f"Origem........: "
        f"{source_catalog}.{source_schema}"
    )

    print(
        f"Controller....: "
        f"{catalog}.{CONTROL_SCHEMA}"
    )

    print(
        "============================================================"
    )

    # --------------------------------------------------------
    # 1. Controller
    # --------------------------------------------------------

    assert_controller_prerequisites(
        spark=spark,
        catalog=catalog,
    )

    # --------------------------------------------------------
    # 2. Catálogo técnico de colunas
    # --------------------------------------------------------

    create_source_column(
        spark=spark,
        catalog=catalog,
    )

    apply_source_column_comments(
        spark=spark,
        catalog=catalog,
    )

    print(
        "source_column criada/validada e documentada."
    )

    # --------------------------------------------------------
    # 3. Discovery das tabelas
    # --------------------------------------------------------

    discovered_tables = (
        discover_source_tables(
            spark=spark,
            source_catalog=source_catalog,
            source_schema=source_schema,
        )
    )

    validate_configured_objects(
        discovered_tables=discovered_tables,
    )

    print(
        f"Tabelas encontradas na origem: "
        f"{len(discovered_tables)}"
    )

    # --------------------------------------------------------
    # 4. Seed do sistema
    # --------------------------------------------------------

    seed_source_system(
        spark=spark,
        catalog=catalog,
        source_catalog=source_catalog,
        source_system_key=source_system_key,
        source_connection=source_connection,
    )

    # --------------------------------------------------------
    # 5. Seed dos objetos
    # --------------------------------------------------------

    seed_source_objects(
        spark=spark,
        catalog=catalog,
        source_catalog=source_catalog,
        source_schema=source_schema,
        source_system_key=source_system_key,
        landing_volume=landing_volume,
    )

    # --------------------------------------------------------
    # 6. Discovery das colunas
    # --------------------------------------------------------

    discovered_columns = (
        discover_source_columns(
            spark=spark,
            source_catalog=source_catalog,
            source_schema=source_schema,
        )
    )

    print(
        f"Colunas descobertas: "
        f"{len(discovered_columns)}"
    )

    # --------------------------------------------------------
    # 7. Validação das chaves configuradas
    # --------------------------------------------------------

    validate_source_key_columns(
        discovered_rows=discovered_columns,
    )

    # --------------------------------------------------------
    # 8. Seed das colunas
    # --------------------------------------------------------

    seed_source_columns(
        spark=spark,
        catalog=catalog,
        source_schema=source_schema,
        source_system_key=source_system_key,
        discovered_rows=discovered_columns,
    )

    # --------------------------------------------------------
    # 9. Validação final
    # --------------------------------------------------------

    validate_seed(
        spark=spark,
        catalog=catalog,
        source_system_key=source_system_key,
    )

    print(
        "============================================================"
    )

    print(
        "Seed de metadados concluído com sucesso."
    )

    print(
        "============================================================"
    )


if __name__ == "__main__":
    main()
import argparse
import re
import uuid
from datetime import date, datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
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

CURSOR_CANDIDATE = "update_date"

PROFILE_TABLE = "incremental_profile"


# ============================================================
# ARGUMENTOS
# ============================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Executa profiling técnico dos candidatos "
            "a cursor incremental cadastrados no Controller."
        )
    )

    parser.add_argument(
        "--catalog",
        required=True,
        help="Catálogo gerenciado do projeto.",
    )

    return parser.parse_args()


# ============================================================
# VALIDAÇÕES
# ============================================================

def validate_identifier(value: str) -> str:
    """
    Valida identificadores simples utilizados em nomes SQL.
    """

    if not re.fullmatch(
        r"[A-Za-z0-9_]+",
        value,
    ):
        raise ValueError(
            f"Identificador inválido: {value}"
        )

    return value


def validate_source_fqn(value: str) -> str:
    """
    Garante que source_fqn possua exatamente:

        catalog.schema.table

    utilizando somente identificadores simples.
    """

    parts = value.split(".")

    if len(parts) != 3:
        raise ValueError(
            f"source_fqn inválido: {value}"
        )

    for part in parts:
        validate_identifier(part)

    return value


def quote_fqn(value: str) -> str:
    """
    Converte catalog.schema.table para uma referência
    SQL devidamente delimitada.
    """

    validate_source_fqn(value)

    catalog, schema, table = value.split(".")

    return (
        f"`{catalog}`."
        f"`{schema}`."
        f"`{table}`"
    )


def escape_sql_literal(value: str) -> str:
    """
    Escapa aspas simples para literais SQL.
    """

    return value.replace(
        "'",
        "''",
    )


# ============================================================
# UTILITÁRIOS
# ============================================================

def format_scalar(value):
    """
    Serializa valores de cursor para armazenamento textual.

    O Controller preserva também cursor_data_type, permitindo
    interpretar corretamente o valor posteriormente.
    """

    if value is None:
        return None

    if isinstance(
        value,
        (datetime, date),
    ):
        return value.isoformat()

    return str(value)


def is_supported_cursor_type(
    data_type: str,
) -> bool:
    """
    Avalia se o tipo técnico é compatível com um cursor
    incremental temporal ou numérico.
    """

    normalized = (
        data_type
        .strip()
        .lower()
    )

    if "timestamp" in normalized:
        return True

    if normalized == "date":
        return True

    numeric_types = {
        "tinyint",
        "smallint",
        "int",
        "integer",
        "bigint",
        "long",
        "float",
        "double",
        "real",
        "numeric",
        "decimal",
    }

    if normalized in numeric_types:
        return True

    if normalized.startswith("decimal"):
        return True

    if normalized.startswith("numeric"):
        return True

    return False


# ============================================================
# CRIAÇÃO DA TABELA DE PROFILE
# ============================================================

def create_profile_table(
    spark: SparkSession,
    catalog: str,
):
    """
    Cria histórico de profiling dos candidatos
    a cursor incremental.
    """

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS
            `{catalog}`.`{CONTROL_SCHEMA}`.`{PROFILE_TABLE}`
        (
            profile_run_id STRING NOT NULL,

            source_object_key STRING NOT NULL,

            source_fqn STRING NOT NULL,

            cursor_column STRING,

            cursor_data_type STRING,

            row_count BIGINT NOT NULL,

            cursor_null_count BIGINT NOT NULL,

            cursor_distinct_count BIGINT NOT NULL,

            repeated_cursor_value_count BIGINT NOT NULL,

            rows_with_repeated_cursor BIGINT NOT NULL,

            rows_at_max_cursor BIGINT NOT NULL,

            cursor_min_value STRING,

            cursor_max_value STRING,

            assessment_status STRING NOT NULL,

            assessment_reason STRING NOT NULL,

            profiled_at TIMESTAMP NOT NULL
        )
        USING DELTA

        COMMENT
            'Histórico de profiling técnico dos candidatos a cursor '
            'incremental. Registra volume, nulabilidade, cardinalidade, '
            'empates de cursor, limites observados e avaliação baseada '
            'no snapshot atual da origem.'
        """
    )


# ============================================================
# DOCUMENTAÇÃO
# ============================================================

def apply_profile_comments(
    spark: SparkSession,
    catalog: str,
):
    """
    Documenta todas as colunas publicadas pela tabela de profile.
    """

    table_fqn = (
        f"`{catalog}`."
        f"`{CONTROL_SCHEMA}`."
        f"`{PROFILE_TABLE}`"
    )

    comments = {
        "profile_run_id": (
            "Identificador único da execução de profiling. "
            "Todas as tabelas analisadas na mesma execução "
            "compartilham este identificador."
        ),

        "source_object_key": (
            "Identificador técnico do objeto de origem avaliado."
        ),

        "source_fqn": (
            "Nome totalmente qualificado do objeto analisado "
            "no catálogo federado."
        ),

        "cursor_column": (
            "Coluna analisada como candidata a cursor incremental. "
            "NULL indica ausência do candidato esperado."
        ),

        "cursor_data_type": (
            "Tipo de dado reportado pelo catálogo técnico "
            "para a coluna candidata."
        ),

        "row_count": (
            "Quantidade total de registros observados "
            "no snapshot analisado."
        ),

        "cursor_null_count": (
            "Quantidade de registros cujo valor da coluna "
            "candidata está nulo."
        ),

        "cursor_distinct_count": (
            "Quantidade de valores distintos não nulos "
            "observados na coluna candidata."
        ),

        "repeated_cursor_value_count": (
            "Quantidade de valores distintos do cursor que aparecem "
            "em mais de um registro."
        ),

        "rows_with_repeated_cursor": (
            "Quantidade total de registros pertencentes a grupos "
            "em que o valor do cursor é compartilhado por "
            "mais de um registro."
        ),

        "rows_at_max_cursor": (
            "Quantidade de registros que compartilham o maior "
            "valor de cursor observado no snapshot."
        ),

        "cursor_min_value": (
            "Menor valor de cursor observado, serializado como texto."
        ),

        "cursor_max_value": (
            "Maior valor de cursor observado, serializado como texto."
        ),

        "assessment_status": (
            "Resultado técnico do profiling baseado no snapshot atual. "
            "Valores possíveis nesta etapa: CANDIDATE, REVIEW "
            "ou NOT_CANDIDATE."
        ),

        "assessment_reason": (
            "Justificativa técnica da classificação atribuída "
            "ao candidato de cursor."
        ),

        "profiled_at": (
            "Timestamp da execução do profiling."
        ),
    }

    for column_name, comment in comments.items():

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
# DISCOVERY DOS OBJETOS
# ============================================================

def discover_objects(
    spark: SparkSession,
    catalog: str,
):
    """
    Descobre automaticamente objetos habilitados e procura
    o candidato update_date no catálogo source_column.

    Não existe lista de tabelas hardcoded neste Job.
    """

    cursor_literal = escape_sql_literal(
        CURSOR_CANDIDATE
    )

    rows = spark.sql(
        f"""
        SELECT
            so.source_object_key,
            so.source_fqn,
            sc.column_name AS cursor_column,
            sc.source_data_type AS cursor_data_type

        FROM
            `{catalog}`.`{CONTROL_SCHEMA}`.`source_object` AS so

        LEFT JOIN
            `{catalog}`.`{CONTROL_SCHEMA}`.`source_column` AS sc

            ON  so.source_object_key = sc.source_object_key
            AND sc.active = TRUE
            AND lower(sc.column_name) = '{cursor_literal}'

        WHERE
            so.enabled = TRUE

        ORDER BY
            so.source_object_key
        """
    ).collect()

    if not rows:
        raise RuntimeError(
            "Nenhum objeto habilitado encontrado no Controller."
        )

    return rows


# ============================================================
# PROFILE DE UM OBJETO
# ============================================================

def profile_object(
    spark: SparkSession,
    source_object_key: str,
    source_fqn: str,
    cursor_column,
    cursor_data_type,
):
    """
    Executa profiling de um único objeto.

    Importante:
    esta análise avalia somente o snapshot atual.

    Ela NÃO consegue provar que update_date será atualizado
    corretamente em todas as mutações futuras da origem.
    """

    quoted_source_fqn = quote_fqn(
        source_fqn
    )

    # --------------------------------------------------------
    # Coluna candidata ausente
    # --------------------------------------------------------

    if cursor_column is None:

        row_count = spark.sql(
            f"""
            SELECT COUNT(*) AS qtd
            FROM {quoted_source_fqn}
            """
        ).first()["qtd"]

        return {
            "source_object_key": source_object_key,
            "source_fqn": source_fqn,
            "cursor_column": None,
            "cursor_data_type": None,
            "row_count": int(row_count),
            "cursor_null_count": 0,
            "cursor_distinct_count": 0,
            "repeated_cursor_value_count": 0,
            "rows_with_repeated_cursor": 0,
            "rows_at_max_cursor": 0,
            "cursor_min_value": None,
            "cursor_max_value": None,
            "assessment_status": "NOT_CANDIDATE",
            "assessment_reason": (
                f"A coluna candidata {CURSOR_CANDIDATE} "
                "não foi encontrada no objeto."
            ),
        }

    validate_identifier(
        cursor_column
    )

    # --------------------------------------------------------
    # Tipo incompatível
    # --------------------------------------------------------

    type_supported = is_supported_cursor_type(
        cursor_data_type
    )

    # --------------------------------------------------------
    # DataFrame da origem
    # --------------------------------------------------------

    source_df = spark.table(
        source_fqn
    )

    cursor_df = source_df.select(
        F.col(cursor_column).alias(
            "cursor_value"
        )
    )

    # --------------------------------------------------------
    # Estatísticas principais
    # --------------------------------------------------------

    base_stats = cursor_df.agg(

        F.count(
            F.lit(1)
        ).cast(
            LongType()
        ).alias(
            "row_count"
        ),

        F.sum(
            F.when(
                F.col(
                    "cursor_value"
                ).isNull(),
                F.lit(1),
            ).otherwise(
                F.lit(0)
            )
        ).cast(
            LongType()
        ).alias(
            "cursor_null_count"
        ),

        F.countDistinct(
            F.col(
                "cursor_value"
            )
        ).cast(
            LongType()
        ).alias(
            "cursor_distinct_count"
        ),

        F.min(
            F.col(
                "cursor_value"
            )
        ).alias(
            "cursor_min"
        ),

        F.max(
            F.col(
                "cursor_value"
            )
        ).alias(
            "cursor_max"
        ),

    ).first()

    row_count = int(
        base_stats["row_count"]
    )

    cursor_null_count = int(
        base_stats["cursor_null_count"] or 0
    )

    cursor_distinct_count = int(
        base_stats[
            "cursor_distinct_count"
        ] or 0
    )

    cursor_min = base_stats[
        "cursor_min"
    ]

    cursor_max = base_stats[
        "cursor_max"
    ]

    # --------------------------------------------------------
    # Empates no valor do cursor
    # --------------------------------------------------------

    cursor_frequency = (
        cursor_df
        .where(
            F.col(
                "cursor_value"
            ).isNotNull()
        )
        .groupBy(
            "cursor_value"
        )
        .count()
    )

    repetition_stats = (
        cursor_frequency
        .agg(

            F.coalesce(
                F.sum(
                    F.when(
                        F.col("count") > 1,
                        F.lit(1),
                    ).otherwise(
                        F.lit(0)
                    )
                ),
                F.lit(0),
            ).cast(
                LongType()
            ).alias(
                "repeated_cursor_value_count"
            ),

            F.coalesce(
                F.sum(
                    F.when(
                        F.col("count") > 1,
                        F.col("count"),
                    ).otherwise(
                        F.lit(0)
                    )
                ),
                F.lit(0),
            ).cast(
                LongType()
            ).alias(
                "rows_with_repeated_cursor"
            ),

        )
        .first()
    )

    repeated_cursor_value_count = int(
        repetition_stats[
            "repeated_cursor_value_count"
        ] or 0
    )

    rows_with_repeated_cursor = int(
        repetition_stats[
            "rows_with_repeated_cursor"
        ] or 0
    )

    # --------------------------------------------------------
    # Quantidade de linhas no maior cursor
    # --------------------------------------------------------

    if cursor_max is None:

        rows_at_max_cursor = 0

    else:

        rows_at_max_cursor = (
            cursor_df
            .where(
                F.col(
                    "cursor_value"
                )
                == F.lit(
                    cursor_max
                )
            )
            .count()
        )

    # --------------------------------------------------------
    # Avaliação
    # --------------------------------------------------------

    if not type_supported:

        assessment_status = (
            "NOT_CANDIDATE"
        )

        assessment_reason = (
            "O tipo da coluna candidata não está entre "
            "os tipos temporais ou numéricos aceitos "
            "para uma estratégia de watermark."
        )

    elif row_count == 0:

        assessment_status = (
            "REVIEW"
        )

        assessment_reason = (
            "A coluna possui tipo compatível, porém a tabela "
            "está vazia. Não existe evidência de dados suficiente "
            "para avaliar o cursor."
        )

    elif cursor_null_count > 0:

        assessment_status = (
            "REVIEW"
        )

        assessment_reason = (
            "A coluna possui tipo compatível, porém existem "
            f"{cursor_null_count} registros com cursor nulo."
        )

    else:

        assessment_status = (
            "CANDIDATE"
        )

        if repeated_cursor_value_count > 0:

            assessment_reason = (
                "A coluna possui tipo compatível e não contém NULLs. "
                f"Foram encontrados {repeated_cursor_value_count} "
                "valores de cursor compartilhados por múltiplas linhas. "
                "Isso não elimina a candidatura, mas exige estratégia "
                "de lookback/overlap para evitar perda em fronteiras "
                "de watermark. O snapshot não prova monotonicidade "
                "operacional futura."
            )

        else:

            assessment_reason = (
                "A coluna possui tipo compatível, não contém NULLs "
                "e não apresentou empates neste snapshot. "
                "O snapshot não prova monotonicidade operacional futura."
            )

    return {
        "source_object_key": source_object_key,
        "source_fqn": source_fqn,
        "cursor_column": cursor_column,
        "cursor_data_type": cursor_data_type,
        "row_count": row_count,
        "cursor_null_count": cursor_null_count,
        "cursor_distinct_count": cursor_distinct_count,
        "repeated_cursor_value_count": (
            repeated_cursor_value_count
        ),
        "rows_with_repeated_cursor": (
            rows_with_repeated_cursor
        ),
        "rows_at_max_cursor": int(
            rows_at_max_cursor
        ),
        "cursor_min_value": format_scalar(
            cursor_min
        ),
        "cursor_max_value": format_scalar(
            cursor_max
        ),
        "assessment_status": assessment_status,
        "assessment_reason": assessment_reason,
    }


# ============================================================
# EXECUÇÃO DO PROFILE
# ============================================================

def execute_profile(
    spark: SparkSession,
    catalog: str,
):
    """
    Executa o profiling de todos os objetos habilitados.
    """

    profile_run_id = str(
        uuid.uuid4()
    )

    profiled_at = datetime.utcnow()

    objects = discover_objects(
        spark=spark,
        catalog=catalog,
    )

    print(
        f"Objetos habilitados encontrados: "
        f"{len(objects)}"
    )

    print(
        f"profile_run_id: {profile_run_id}"
    )

    results = []

    for source_object in objects:

        source_object_key = (
            source_object[
                "source_object_key"
            ]
        )

        source_fqn = (
            source_object[
                "source_fqn"
            ]
        )

        cursor_column = (
            source_object[
                "cursor_column"
            ]
        )

        cursor_data_type = (
            source_object[
                "cursor_data_type"
            ]
        )

        print(
            "------------------------------------------------------------"
        )

        print(
            f"Profiling: {source_object_key}"
        )

        result = profile_object(
            spark=spark,
            source_object_key=source_object_key,
            source_fqn=source_fqn,
            cursor_column=cursor_column,
            cursor_data_type=cursor_data_type,
        )

        print(
            f"  status.........: "
            f"{result['assessment_status']}"
        )

        print(
            f"  rows...........: "
            f"{result['row_count']}"
        )

        print(
            f"  nulls..........: "
            f"{result['cursor_null_count']}"
        )

        print(
            f"  distinct.......: "
            f"{result['cursor_distinct_count']}"
        )

        print(
            f"  repeated values: "
            f"{result['repeated_cursor_value_count']}"
        )

        print(
            f"  rows at max....: "
            f"{result['rows_at_max_cursor']}"
        )

        print(
            f"  min............: "
            f"{result['cursor_min_value']}"
        )

        print(
            f"  max............: "
            f"{result['cursor_max_value']}"
        )

        results.append(
            (
                profile_run_id,
                result[
                    "source_object_key"
                ],
                result[
                    "source_fqn"
                ],
                result[
                    "cursor_column"
                ],
                result[
                    "cursor_data_type"
                ],
                result[
                    "row_count"
                ],
                result[
                    "cursor_null_count"
                ],
                result[
                    "cursor_distinct_count"
                ],
                result[
                    "repeated_cursor_value_count"
                ],
                result[
                    "rows_with_repeated_cursor"
                ],
                result[
                    "rows_at_max_cursor"
                ],
                result[
                    "cursor_min_value"
                ],
                result[
                    "cursor_max_value"
                ],
                result[
                    "assessment_status"
                ],
                result[
                    "assessment_reason"
                ],
                profiled_at,
            )
        )

    return (
        profile_run_id,
        results,
    )


# ============================================================
# PERSISTÊNCIA
# ============================================================

def persist_results(
    spark: SparkSession,
    catalog: str,
    results,
):
    """
    Persiste historicamente os resultados do profile.

    A estratégia é append:
    cada execução constitui uma nova evidência técnica.
    """

    dataframe_schema = StructType(
        [
            StructField(
                "profile_run_id",
                StringType(),
                False,
            ),
            StructField(
                "source_object_key",
                StringType(),
                False,
            ),
            StructField(
                "source_fqn",
                StringType(),
                False,
            ),
            StructField(
                "cursor_column",
                StringType(),
                True,
            ),
            StructField(
                "cursor_data_type",
                StringType(),
                True,
            ),
            StructField(
                "row_count",
                LongType(),
                False,
            ),
            StructField(
                "cursor_null_count",
                LongType(),
                False,
            ),
            StructField(
                "cursor_distinct_count",
                LongType(),
                False,
            ),
            StructField(
                "repeated_cursor_value_count",
                LongType(),
                False,
            ),
            StructField(
                "rows_with_repeated_cursor",
                LongType(),
                False,
            ),
            StructField(
                "rows_at_max_cursor",
                LongType(),
                False,
            ),
            StructField(
                "cursor_min_value",
                StringType(),
                True,
            ),
            StructField(
                "cursor_max_value",
                StringType(),
                True,
            ),
            StructField(
                "assessment_status",
                StringType(),
                False,
            ),
            StructField(
                "assessment_reason",
                StringType(),
                False,
            ),
            StructField(
                "profiled_at",
                TimestampType(),
                False,
            ),
        ]
    )

    df = spark.createDataFrame(
        results,
        schema=dataframe_schema,
    )

    df.createOrReplaceTempView(
        "incremental_profile_results"
    )

    spark.sql(
        f"""
        INSERT INTO
            `{catalog}`.`{CONTROL_SCHEMA}`.`{PROFILE_TABLE}`

        SELECT
            profile_run_id,
            source_object_key,
            source_fqn,
            cursor_column,
            cursor_data_type,
            row_count,
            cursor_null_count,
            cursor_distinct_count,
            repeated_cursor_value_count,
            rows_with_repeated_cursor,
            rows_at_max_cursor,
            cursor_min_value,
            cursor_max_value,
            assessment_status,
            assessment_reason,
            profiled_at

        FROM
            incremental_profile_results
        """
    )


# ============================================================
# VALIDAÇÃO FINAL
# ============================================================

def validate_profile(
    spark: SparkSession,
    catalog: str,
    profile_run_id: str,
):
    """
    Valida se todos os objetos habilitados produziram
    um resultado de profiling.
    """

    escaped_run_id = escape_sql_literal(
        profile_run_id
    )

    expected_count = spark.sql(
        f"""
        SELECT COUNT(*) AS qtd
        FROM
            `{catalog}`.`{CONTROL_SCHEMA}`.`source_object`
        WHERE
            enabled = TRUE
        """
    ).first()["qtd"]

    persisted_count = spark.sql(
        f"""
        SELECT COUNT(*) AS qtd
        FROM
            `{catalog}`.`{CONTROL_SCHEMA}`.`{PROFILE_TABLE}`
        WHERE
            profile_run_id = '{escaped_run_id}'
        """
    ).first()["qtd"]

    if expected_count != persisted_count:
        raise RuntimeError(
            "Quantidade de profiles persistidos diferente "
            "da quantidade de objetos habilitados. "
            f"Esperado={expected_count}, "
            f"Persistido={persisted_count}"
        )

    status_rows = spark.sql(
        f"""
        SELECT
            assessment_status,
            COUNT(*) AS qtd

        FROM
            `{catalog}`.`{CONTROL_SCHEMA}`.`{PROFILE_TABLE}`

        WHERE
            profile_run_id = '{escaped_run_id}'

        GROUP BY
            assessment_status

        ORDER BY
            assessment_status
        """
    ).collect()

    print(
        "============================================================"
    )

    print(
        "Resumo da avaliação:"
    )

    for row in status_rows:

        print(
            f"  {row['assessment_status']}: "
            f"{row['qtd']}"
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
        "Profiling dos candidatos a cursor incremental"
    )

    print(
        f"Controller: "
        f"{catalog}.{CONTROL_SCHEMA}"
    )

    print(
        f"Candidato: {CURSOR_CANDIDATE}"
    )

    print(
        "============================================================"
    )

    # --------------------------------------------------------
    # 1. Estrutura de persistência
    # --------------------------------------------------------

    create_profile_table(
        spark=spark,
        catalog=catalog,
    )

    apply_profile_comments(
        spark=spark,
        catalog=catalog,
    )

    # --------------------------------------------------------
    # 2. Profiling
    # --------------------------------------------------------

    (
        profile_run_id,
        results,
    ) = execute_profile(
        spark=spark,
        catalog=catalog,
    )

    # --------------------------------------------------------
    # 3. Persistência histórica
    # --------------------------------------------------------

    persist_results(
        spark=spark,
        catalog=catalog,
        results=results,
    )

    # --------------------------------------------------------
    # 4. Validação
    # --------------------------------------------------------

    validate_profile(
        spark=spark,
        catalog=catalog,
        profile_run_id=profile_run_id,
    )

    print(
        f"profile_run_id concluído: "
        f"{profile_run_id}"
    )

    print(
        "Profiling concluído com sucesso."
    )


if __name__ == "__main__":
    main()
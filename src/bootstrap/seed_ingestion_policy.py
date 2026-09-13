import argparse
import re
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.types import (
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

CURSOR_COLUMN = "update_date"
CURSOR_LOGICAL_DATA_TYPE = "TIMESTAMP"

LANDING_FORMAT = "PARQUET"

DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_INTERVAL_SECONDS = 60

# O profiling demonstrou baixa cardinalidade temporal.
#
# Começaremos com D-1 para absorver empates na fronteira
# do watermark sem transformar a ingestão incremental
# em uma releitura quase completa.
#
# Esse valor deverá futuramente ser ajustado com base
# em métricas reais de atraso/late arrival.
DEFAULT_INCREMENTAL_LOOKBACK_DAYS = 1


# ============================================================
# POLÍTICAS DECLARATIVAS DO LABORATÓRIO
# ============================================================

POLICIES = {

    # --------------------------------------------------------
    # Objetos pequenos / referência
    # --------------------------------------------------------

    "agencias": {
        "bootstrap_mode": "FULL",
        "steady_state_mode": "FULL",
        "incremental_column": None,
        "watermark_data_type": None,
        "lookback_days": 0,
        "schedule_group": "DAILY_FULL",
    },

    "produtos_credito": {
        "bootstrap_mode": "FULL",
        "steady_state_mode": "FULL",
        "incremental_column": None,
        "watermark_data_type": None,
        "lookback_days": 0,
        "schedule_group": "DAILY_FULL",
    },

    # --------------------------------------------------------
    # Objetos incrementais
    # --------------------------------------------------------

    "atendimentos_relacionamento": {
        "bootstrap_mode": "FULL",
        "steady_state_mode": "INCREMENTAL",
        "incremental_column": CURSOR_COLUMN,
        "watermark_data_type": CURSOR_LOGICAL_DATA_TYPE,
        "lookback_days": DEFAULT_INCREMENTAL_LOOKBACK_DAYS,
        "schedule_group": "DAILY_INCREMENTAL",
    },

    "contas_correntes": {
        "bootstrap_mode": "FULL",
        "steady_state_mode": "INCREMENTAL",
        "incremental_column": CURSOR_COLUMN,
        "watermark_data_type": CURSOR_LOGICAL_DATA_TYPE,
        "lookback_days": DEFAULT_INCREMENTAL_LOOKBACK_DAYS,
        "schedule_group": "DAILY_INCREMENTAL",
    },

    "contratos_credito": {
        "bootstrap_mode": "FULL",
        "steady_state_mode": "INCREMENTAL",
        "incremental_column": CURSOR_COLUMN,
        "watermark_data_type": CURSOR_LOGICAL_DATA_TYPE,
        "lookback_days": DEFAULT_INCREMENTAL_LOOKBACK_DAYS,
        "schedule_group": "DAILY_INCREMENTAL",
    },

    "cooperados": {
        "bootstrap_mode": "FULL",
        "steady_state_mode": "INCREMENTAL",
        "incremental_column": CURSOR_COLUMN,
        "watermark_data_type": CURSOR_LOGICAL_DATA_TYPE,
        "lookback_days": DEFAULT_INCREMENTAL_LOOKBACK_DAYS,
        "schedule_group": "DAILY_INCREMENTAL",
    },

    "investimentos": {
        "bootstrap_mode": "FULL",
        "steady_state_mode": "INCREMENTAL",
        "incremental_column": CURSOR_COLUMN,
        "watermark_data_type": CURSOR_LOGICAL_DATA_TYPE,
        "lookback_days": DEFAULT_INCREMENTAL_LOOKBACK_DAYS,
        "schedule_group": "DAILY_INCREMENTAL",
    },

    "lancamentos_contabeis": {
        "bootstrap_mode": "FULL",
        "steady_state_mode": "INCREMENTAL",
        "incremental_column": CURSOR_COLUMN,
        "watermark_data_type": CURSOR_LOGICAL_DATA_TYPE,
        "lookback_days": DEFAULT_INCREMENTAL_LOOKBACK_DAYS,
        "schedule_group": "DAILY_INCREMENTAL",
    },

    "parcelas_credito": {
        "bootstrap_mode": "FULL",
        "steady_state_mode": "INCREMENTAL",
        "incremental_column": CURSOR_COLUMN,
        "watermark_data_type": CURSOR_LOGICAL_DATA_TYPE,
        "lookback_days": DEFAULT_INCREMENTAL_LOOKBACK_DAYS,
        "schedule_group": "DAILY_INCREMENTAL",
    },

    "score_risco_cooperado": {
        "bootstrap_mode": "FULL",
        "steady_state_mode": "INCREMENTAL",
        "incremental_column": CURSOR_COLUMN,
        "watermark_data_type": CURSOR_LOGICAL_DATA_TYPE,
        "lookback_days": DEFAULT_INCREMENTAL_LOOKBACK_DAYS,
        "schedule_group": "DAILY_INCREMENTAL",
    },
}


# ============================================================
# ARGUMENTOS
# ============================================================

def parse_args():

    parser = argparse.ArgumentParser(
        description=(
            "Realiza o seed idempotente das políticas "
            "de ingestão do Controller."
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
    Valida identificadores utilizados na composição
    de nomes de objetos SQL.
    """

    if not re.fullmatch(
        r"[A-Za-z0-9_]+",
        value,
    ):
        raise ValueError(
            f"Identificador inválido: {value}"
        )

    return value


def escape_sql_literal(value: str) -> str:
    """
    Escapa aspas simples utilizadas em literais SQL.
    """

    return value.replace(
        "'",
        "''",
    )


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


# ============================================================
# DISCOVERY DOS OBJETOS
# ============================================================

def discover_enabled_objects(
    spark: SparkSession,
    catalog: str,
):
    """
    Obtém os objetos atualmente habilitados no Controller.
    """

    rows = spark.sql(
        f"""
        SELECT
            source_object_key,
            source_table
        FROM
            `{catalog}`.`{CONTROL_SCHEMA}`.`source_object`
        WHERE
            enabled = TRUE
        ORDER BY
            source_table
        """
    ).collect()

    if not rows:

        raise RuntimeError(
            "Nenhum source_object habilitado foi encontrado."
        )

    return rows


# ============================================================
# VALIDAÇÃO DAS POLÍTICAS CONFIGURADAS
# ============================================================

def validate_policy_configuration(
    spark: SparkSession,
    catalog: str,
):
    """
    Compara os objetos habilitados no Controller
    com as políticas declaradas neste arquivo.

    Nenhum objeto habilitado pode ficar sem política
    e nenhuma política pode apontar para objeto inexistente.
    """

    objects = discover_enabled_objects(
        spark=spark,
        catalog=catalog,
    )

    discovered_tables = {
        row["source_table"]
        for row in objects
    }

    configured_tables = set(
        POLICIES.keys()
    )

    missing_policies = (
        discovered_tables
        - configured_tables
    )

    unknown_policies = (
        configured_tables
        - discovered_tables
    )

    if missing_policies:

        raise RuntimeError(
            "Existem objetos habilitados sem política configurada: "
            f"{sorted(missing_policies)}"
        )

    if unknown_policies:

        raise RuntimeError(
            "Existem políticas configuradas para objetos "
            "não encontrados no Controller: "
            f"{sorted(unknown_policies)}"
        )

    if len(objects) != len(POLICIES):

        raise RuntimeError(
            "Quantidade de objetos habilitados diferente "
            "da quantidade de políticas configuradas. "
            f"Objetos={len(objects)}, "
            f"políticas={len(POLICIES)}"
        )

    print(
        "Contrato objeto/política validado."
    )

    return objects


# ============================================================
# VALIDAÇÃO DA ESTRUTURA DOS CURSORES
# ============================================================

def validate_incremental_columns(
    spark: SparkSession,
    catalog: str,
):
    """
    Para cada objeto configurado como INCREMENTAL, valida:

      1. existência de update_date;
      2. coluna ativa no catálogo técnico;
      3. coluna NOT NULL;
      4. tipo timestamp;
      5. existência de profiling;
      6. cursor_null_count = 0;
      7. assessment_status = CANDIDATE.

    A existência de CANDIDATE não significa prova absoluta
    de monotonicidade futura. Ela representa aprovação
    técnica com base no snapshot observado.
    """

    incremental_tables = sorted(
        table_name
        for table_name, policy
        in POLICIES.items()
        if (
            policy["steady_state_mode"]
            == "INCREMENTAL"
        )
    )

    incremental_filter = ", ".join(
        f"'{escape_sql_literal(table_name)}'"
        for table_name
        in incremental_tables
    )

    # --------------------------------------------------------
    # 1. Validação estrutural no source_column
    # --------------------------------------------------------

    structural_rows = spark.sql(
        f"""
        SELECT
            so.source_table,
            sc.column_name,
            sc.source_data_type,
            sc.is_nullable,
            sc.active

        FROM
            `{catalog}`.`{CONTROL_SCHEMA}`.`source_object`
            AS so

        LEFT JOIN
            `{catalog}`.`{CONTROL_SCHEMA}`.`source_column`
            AS sc

            ON
                sc.source_object_key
                =
                so.source_object_key

            AND
                lower(sc.column_name)
                =
                '{CURSOR_COLUMN}'

        WHERE
            so.enabled = TRUE

            AND so.source_table IN (
                {incremental_filter}
            )
        """
    ).collect()

    structural_by_table = {
        row["source_table"]: row
        for row
        in structural_rows
    }

    errors = []

    for table_name in incremental_tables:

        row = structural_by_table.get(
            table_name
        )

        if row is None:

            errors.append(
                f"{table_name}: metadata do cursor ausente"
            )

            continue

        if row["column_name"] is None:

            errors.append(
                f"{table_name}: "
                f"{CURSOR_COLUMN} não encontrada"
            )

            continue

        if not row["active"]:

            errors.append(
                f"{table_name}: "
                f"{CURSOR_COLUMN} está inativa"
            )

        if row["is_nullable"]:

            errors.append(
                f"{table_name}: "
                f"{CURSOR_COLUMN} permite NULL"
            )

        source_type = (
            str(
                row["source_data_type"]
            )
            .strip()
            .lower()
        )

        if "timestamp" not in source_type:

            errors.append(
                f"{table_name}: "
                f"tipo inesperado para "
                f"{CURSOR_COLUMN}: "
                f"{row['source_data_type']}"
            )

    if errors:

        raise RuntimeError(
            "Falha na validação estrutural dos cursores: "
            + "; ".join(errors)
        )

    print(
        "Estrutura dos cursores incrementais validada."
    )

    # --------------------------------------------------------
    # 2. Descobre a execução completa mais recente do profile
    # --------------------------------------------------------

    latest_profile = spark.sql(
        f"""
        SELECT
            profile_run_id,
            MAX(profiled_at) AS profiled_at

        FROM
            `{catalog}`.`{CONTROL_SCHEMA}`.`incremental_profile`

        GROUP BY
            profile_run_id

        ORDER BY
            profiled_at DESC

        LIMIT 1
        """
    ).first()

    if latest_profile is None:

        raise RuntimeError(
            "Nenhum incremental_profile foi encontrado. "
            "Execute controller_profile_incremental "
            "antes de cadastrar ingestion_policy."
        )

    profile_run_id = (
        latest_profile[
            "profile_run_id"
        ]
    )

    escaped_profile_run_id = (
        escape_sql_literal(
            profile_run_id
        )
    )

    # --------------------------------------------------------
    # 3. Validação da evidência do profile
    # --------------------------------------------------------

    profile_rows = spark.sql(
        f"""
        SELECT
            so.source_table,

            ip.cursor_column,

            ip.cursor_data_type,

            ip.row_count,

            ip.cursor_null_count,

            ip.cursor_distinct_count,

            ip.repeated_cursor_value_count,

            ip.rows_with_repeated_cursor,

            ip.rows_at_max_cursor,

            ip.assessment_status

        FROM
            `{catalog}`.`{CONTROL_SCHEMA}`.`incremental_profile`
            AS ip

        INNER JOIN
            `{catalog}`.`{CONTROL_SCHEMA}`.`source_object`
            AS so

            ON
                so.source_object_key
                =
                ip.source_object_key

        WHERE
            ip.profile_run_id
            =
            '{escaped_profile_run_id}'

            AND so.source_table IN (
                {incremental_filter}
            )
        """
    ).collect()

    profile_by_table = {
        row["source_table"]: row
        for row
        in profile_rows
    }

    errors = []

    for table_name in incremental_tables:

        row = profile_by_table.get(
            table_name
        )

        if row is None:

            errors.append(
                f"{table_name}: profile não encontrado"
            )

            continue

        if (
            row["cursor_column"]
            != CURSOR_COLUMN
        ):

            errors.append(
                f"{table_name}: cursor inesperado "
                f"no profile: {row['cursor_column']}"
            )

        if (
            row["cursor_null_count"]
            != 0
        ):

            errors.append(
                f"{table_name}: "
                f"profile encontrou "
                f"{row['cursor_null_count']} NULLs"
            )

        if (
            row["assessment_status"]
            != "CANDIDATE"
        ):

            errors.append(
                f"{table_name}: "
                f"assessment_status="
                f"{row['assessment_status']}"
            )

    if errors:

        raise RuntimeError(
            "Falha na validação do profiling incremental: "
            + "; ".join(errors)
        )

    print(
        "Profiling dos cursores incrementais validado."
    )

    print(
        f"profile_run_id utilizado: "
        f"{profile_run_id}"
    )

    # --------------------------------------------------------
    # Log informativo dos empates observados
    # --------------------------------------------------------

    print(
        "Resumo dos cursores incrementais:"
    )

    for table_name in incremental_tables:

        row = profile_by_table[
            table_name
        ]

        print(
            f"  {table_name}: "
            f"rows={row['row_count']}, "
            f"distinct_cursor="
            f"{row['cursor_distinct_count']}, "
            f"rows_at_max="
            f"{row['rows_at_max_cursor']}, "
            f"repeated_cursor_values="
            f"{row['repeated_cursor_value_count']}"
        )


# ============================================================
# CONSTRUÇÃO DO SEED
# ============================================================

def build_policy_rows(
    objects,
):
    """
    Constrói os registros que serão persistidos
    em ingestion_policy.
    """

    now = utc_now()

    rows = []

    for source_object in objects:

        source_object_key = (
            source_object[
                "source_object_key"
            ]
        )

        source_table = (
            source_object[
                "source_table"
            ]
        )

        policy = POLICIES[
            source_table
        ]

        rows.append(
            (
                source_object_key,

                policy[
                    "bootstrap_mode"
                ],

                policy[
                    "steady_state_mode"
                ],

                policy[
                    "incremental_column"
                ],

                policy[
                    "watermark_data_type"
                ],

                policy[
                    "lookback_days"
                ],

                policy[
                    "schedule_group"
                ],

                LANDING_FORMAT,

                True,

                DEFAULT_MAX_RETRIES,

                DEFAULT_RETRY_INTERVAL_SECONDS,

                True,

                True,

                True,

                now,

                now,
            )
        )

    return rows


# ============================================================
# DATAFRAME SCHEMA
# ============================================================

def get_policy_dataframe_schema():
    """
    Schema explícito utilizado para construir
    o seed das políticas.
    """

    return StructType(
        [
            StructField(
                "source_object_key",
                StringType(),
                False,
            ),

            StructField(
                "bootstrap_mode",
                StringType(),
                False,
            ),

            StructField(
                "steady_state_mode",
                StringType(),
                False,
            ),

            StructField(
                "incremental_column",
                StringType(),
                True,
            ),

            StructField(
                "watermark_data_type",
                StringType(),
                True,
            ),

            StructField(
                "lookback_days",
                IntegerType(),
                True,
            ),

            StructField(
                "schedule_group",
                StringType(),
                True,
            ),

            StructField(
                "landing_format",
                StringType(),
                False,
            ),

            StructField(
                "retry_enabled",
                BooleanType(),
                False,
            ),

            StructField(
                "max_retries",
                IntegerType(),
                False,
            ),

            StructField(
                "retry_interval_seconds",
                IntegerType(),
                False,
            ),

            StructField(
                "manual_reprocessing_enabled",
                BooleanType(),
                False,
            ),

            StructField(
                "full_refresh_enabled",
                BooleanType(),
                False,
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


# ============================================================
# PERSISTÊNCIA
# ============================================================

def seed_ingestion_policy(
    spark: SparkSession,
    catalog: str,
    rows,
):
    """
    Realiza MERGE idempotente sobre ingestion_policy.

    created_at é preservado em registros existentes.
    updated_at acompanha alterações/reaplicações da política.
    """

    dataframe_schema = (
        get_policy_dataframe_schema()
    )

    df = spark.createDataFrame(
        rows,
        schema=dataframe_schema,
    )

    df.createOrReplaceTempView(
        "seed_ingestion_policy"
    )

    spark.sql(
        f"""
        MERGE INTO
            `{catalog}`.`{CONTROL_SCHEMA}`.`ingestion_policy`
            AS tgt

        USING
            seed_ingestion_policy
            AS src

        ON
            tgt.source_object_key
            =
            src.source_object_key

        WHEN MATCHED THEN
            UPDATE SET

                tgt.bootstrap_mode =
                    src.bootstrap_mode,

                tgt.steady_state_mode =
                    src.steady_state_mode,

                tgt.incremental_column =
                    src.incremental_column,

                tgt.watermark_data_type =
                    src.watermark_data_type,

                tgt.lookback_days =
                    src.lookback_days,

                tgt.schedule_group =
                    src.schedule_group,

                tgt.landing_format =
                    src.landing_format,

                tgt.retry_enabled =
                    src.retry_enabled,

                tgt.max_retries =
                    src.max_retries,

                tgt.retry_interval_seconds =
                    src.retry_interval_seconds,

                tgt.manual_reprocessing_enabled =
                    src.manual_reprocessing_enabled,

                tgt.full_refresh_enabled =
                    src.full_refresh_enabled,

                tgt.enabled =
                    src.enabled,

                tgt.updated_at =
                    src.updated_at

        WHEN NOT MATCHED THEN
            INSERT (
                source_object_key,
                bootstrap_mode,
                steady_state_mode,
                incremental_column,
                watermark_data_type,
                lookback_days,
                schedule_group,
                landing_format,
                retry_enabled,
                max_retries,
                retry_interval_seconds,
                manual_reprocessing_enabled,
                full_refresh_enabled,
                enabled,
                created_at,
                updated_at
            )
            VALUES (
                src.source_object_key,
                src.bootstrap_mode,
                src.steady_state_mode,
                src.incremental_column,
                src.watermark_data_type,
                src.lookback_days,
                src.schedule_group,
                src.landing_format,
                src.retry_enabled,
                src.max_retries,
                src.retry_interval_seconds,
                src.manual_reprocessing_enabled,
                src.full_refresh_enabled,
                src.enabled,
                src.created_at,
                src.updated_at
            )
        """
    )

    print(
        f"ingestion_policy atualizado com "
        f"{len(rows)} políticas."
    )


# ============================================================
# VALIDAÇÃO FINAL
# ============================================================

def validate_seed(
    spark: SparkSession,
    catalog: str,
):
    """
    Confirma quantidade e distribuição final
    das políticas habilitadas.
    """

    total_count = spark.sql(
        f"""
        SELECT
            COUNT(*) AS qtd

        FROM
            `{catalog}`.`{CONTROL_SCHEMA}`.`ingestion_policy`

        WHERE
            enabled = TRUE
        """
    ).first()["qtd"]

    if total_count != len(POLICIES):

        raise RuntimeError(
            "Quantidade inesperada de políticas habilitadas. "
            f"Esperado={len(POLICIES)}, "
            f"encontrado={total_count}"
        )

    summary = spark.sql(
        f"""
        SELECT
            steady_state_mode,
            COUNT(*) AS qtd

        FROM
            `{catalog}`.`{CONTROL_SCHEMA}`.`ingestion_policy`

        WHERE
            enabled = TRUE

        GROUP BY
            steady_state_mode

        ORDER BY
            steady_state_mode
        """
    ).collect()

    summary_dict = {
        row["steady_state_mode"]: row["qtd"]
        for row
        in summary
    }

    expected_full = 2
    expected_incremental = 8

    actual_full = summary_dict.get(
        "FULL",
        0,
    )

    actual_incremental = summary_dict.get(
        "INCREMENTAL",
        0,
    )

    if actual_full != expected_full:

        raise RuntimeError(
            "Quantidade inesperada de políticas FULL. "
            f"Esperado={expected_full}, "
            f"encontrado={actual_full}"
        )

    if (
        actual_incremental
        != expected_incremental
    ):

        raise RuntimeError(
            "Quantidade inesperada de políticas INCREMENTAL. "
            f"Esperado={expected_incremental}, "
            f"encontrado={actual_incremental}"
        )

    # --------------------------------------------------------
    # Valida lookback final
    # --------------------------------------------------------

    invalid_incremental_lookback = spark.sql(
        f"""
        SELECT
            COUNT(*) AS qtd

        FROM
            `{catalog}`.`{CONTROL_SCHEMA}`.`ingestion_policy`

        WHERE
            enabled = TRUE

            AND steady_state_mode = 'INCREMENTAL'

            AND lookback_days
                !=
                {DEFAULT_INCREMENTAL_LOOKBACK_DAYS}
        """
    ).first()["qtd"]

    if invalid_incremental_lookback != 0:

        raise RuntimeError(
            "Existem políticas incrementais com "
            "lookback diferente do esperado."
        )

    invalid_full_policy = spark.sql(
        f"""
        SELECT
            COUNT(*) AS qtd

        FROM
            `{catalog}`.`{CONTROL_SCHEMA}`.`ingestion_policy`

        WHERE
            enabled = TRUE

            AND steady_state_mode = 'FULL'

            AND (
                incremental_column IS NOT NULL
                OR watermark_data_type IS NOT NULL
                OR lookback_days != 0
            )
        """
    ).first()["qtd"]

    if invalid_full_policy != 0:

        raise RuntimeError(
            "Existem políticas FULL com configuração "
            "incremental indevida."
        )

    print(
        "============================================================"
    )

    print(
        "Resumo das políticas:"
    )

    for row in summary:

        print(
            f"  {row['steady_state_mode']}: "
            f"{row['qtd']}"
        )

    print(
        f"  lookback incremental: "
        f"{DEFAULT_INCREMENTAL_LOOKBACK_DAYS} dia"
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
        "Seed das políticas de ingestão"
    )

    print(
        f"Controller: "
        f"{catalog}.{CONTROL_SCHEMA}"
    )

    print(
        "============================================================"
    )

    # --------------------------------------------------------
    # 1. Validação objeto/política
    # --------------------------------------------------------

    objects = validate_policy_configuration(
        spark=spark,
        catalog=catalog,
    )

    # --------------------------------------------------------
    # 2. Validação da evidência técnica
    # --------------------------------------------------------

    validate_incremental_columns(
        spark=spark,
        catalog=catalog,
    )

    # --------------------------------------------------------
    # 3. Construção do seed
    # --------------------------------------------------------

    rows = build_policy_rows(
        objects=objects,
    )

    # --------------------------------------------------------
    # 4. Persistência
    # --------------------------------------------------------

    seed_ingestion_policy(
        spark=spark,
        catalog=catalog,
        rows=rows,
    )

    # --------------------------------------------------------
    # 5. Validação final
    # --------------------------------------------------------

    validate_seed(
        spark=spark,
        catalog=catalog,
    )

    print(
        "Seed de ingestion_policy concluído com sucesso."
    )


if __name__ == "__main__":
    main()
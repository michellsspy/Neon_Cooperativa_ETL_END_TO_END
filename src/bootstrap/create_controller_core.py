import argparse
import re

from pyspark.sql import SparkSession


# ============================================================
# CONSTANTES
# ============================================================

CONTROL_SCHEMA_NAME = "control"

SOURCE_KEY_ORIGINS = (
    "DECLARED",
    "INFERRED",
    "CONFIGURED",
    "NONE",
)

SOURCE_KEY_ORIGIN_CONSTRAINT = "ck_source_key_origin"


# ============================================================
# ARGUMENTOS
# ============================================================

def parse_args():
    """
    Lê os argumentos utilizados pelo Job de bootstrap.
    """

    parser = argparse.ArgumentParser(
        description="Bootstrap das tabelas centrais do Controller de Ingestão."
    )

    parser.add_argument(
        "--catalog",
        required=True,
        help=(
            "Nome do catálogo Unity Catalog onde será utilizado "
            "o schema control."
        ),
    )

    return parser.parse_args()


# ============================================================
# VALIDAÇÕES E UTILITÁRIOS
# ============================================================

def validate_identifier(value: str) -> str:
    """
    Valida identificadores utilizados na composição
    de nomes SQL.

    Permite somente letras, números e underscore.
    """

    if not re.fullmatch(r"[A-Za-z0-9_]+", value):
        raise ValueError(
            f"Identificador inválido para catálogo: {value}"
        )

    return value


def escape_sql_literal(value: str) -> str:
    """
    Escapa aspas simples para uso seguro em literais SQL.
    """

    return value.replace("'", "''")


def table_exists(
    spark: SparkSession,
    catalog: str,
    schema: str,
    table: str,
) -> bool:
    """
    Verifica se uma tabela existe no Unity Catalog.
    """

    result = spark.sql(
        f"""
        SELECT COUNT(*) AS qtd
        FROM `{catalog}`.information_schema.tables
        WHERE table_schema = '{escape_sql_literal(schema)}'
          AND table_name = '{escape_sql_literal(table)}'
        """
    ).first()

    return result["qtd"] > 0


def get_table_columns(
    spark: SparkSession,
    catalog: str,
    schema: str,
    table: str,
) -> set[str]:
    """
    Retorna as colunas existentes em uma tabela.
    """

    rows = spark.sql(
        f"""
        SELECT column_name
        FROM `{catalog}`.information_schema.columns
        WHERE table_schema = '{escape_sql_literal(schema)}'
          AND table_name = '{escape_sql_literal(table)}'
        ORDER BY ordinal_position
        """
    ).collect()

    return {
        row["column_name"]
        for row in rows
    }


def get_table_count(
    spark: SparkSession,
    table_fqn: str,
) -> int:
    """
    Retorna a quantidade de registros de uma tabela.
    """

    result = spark.sql(
        f"""
        SELECT COUNT(*) AS qtd
        FROM {table_fqn}
        """
    ).first()

    return result["qtd"]


# ============================================================
# MIGRAÇÃO DEFENSIVA DE SOURCE_OBJECT
# ============================================================

def migrate_source_object_if_required(
    spark: SparkSession,
    catalog: str,
):
    """
    Trata a evolução do contrato inicial:

        source_primary_key

    para:

        source_key_columns
        source_key_origin

    A remoção automática da tabela antiga somente ocorre
    se ela ainda estiver vazia.

    Caso existam registros, a execução é interrompida para
    impedir perda silenciosa de configurações.
    """

    schema = CONTROL_SCHEMA_NAME
    table = "source_object"

    table_fqn = (
        f"`{catalog}`."
        f"`{schema}`."
        f"`{table}`"
    )

    if not table_exists(
        spark=spark,
        catalog=catalog,
        schema=schema,
        table=table,
    ):
        print(
            "source_object ainda não existe. "
            "Nenhuma migração necessária."
        )
        return

    columns = get_table_columns(
        spark=spark,
        catalog=catalog,
        schema=schema,
        table=table,
    )

    # --------------------------------------------------------
    # Versão antiga detectada
    # --------------------------------------------------------

    if "source_primary_key" in columns:

        record_count = get_table_count(
            spark=spark,
            table_fqn=table_fqn,
        )

        if record_count != 0:
            raise RuntimeError(
                "A tabela source_object utiliza o contrato antigo "
                "com source_primary_key e possui registros. "
                "A migração automática foi bloqueada para evitar "
                "perda de configuração."
            )

        print(
            "Contrato antigo detectado em source_object. "
            "A tabela está vazia e será recriada."
        )

        spark.sql(
            f"""
            DROP TABLE {table_fqn}
            """
        )

        print(
            "Tabela source_object antiga removida com sucesso."
        )

        return

    # --------------------------------------------------------
    # Versão nova
    # --------------------------------------------------------

    required_columns = {
        "source_key_columns",
        "source_key_origin",
    }

    if required_columns.issubset(columns):
        print(
            "source_object já utiliza o contrato atual."
        )
        return

    # --------------------------------------------------------
    # Estado inesperado
    # --------------------------------------------------------

    raise RuntimeError(
        "A tabela source_object existe, porém seu contrato "
        "não corresponde nem à versão antiga nem à versão atual. "
        "A execução foi interrompida para evitar alteração "
        "destrutiva automática."
    )


# ============================================================
# CRIAÇÃO: SOURCE_SYSTEM
# ============================================================

def create_source_system(
    spark: SparkSession,
    control_schema: str,
):
    """
    Cria o cadastro dos sistemas de origem.
    """

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {control_schema}.source_system (

            source_system_key STRING NOT NULL,
            source_type STRING NOT NULL,

            connection_name STRING,
            foreign_catalog STRING,

            description STRING,
            owner STRING,

            enabled BOOLEAN NOT NULL,

            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL,

            CONSTRAINT pk_source_system
                PRIMARY KEY (source_system_key) NOT ENFORCED
        )
        USING DELTA
        COMMENT
            'Cadastro dos sistemas de origem disponíveis para ingestão. '
            'Representa a origem física ou lógica e sua integração '
            'governada com a plataforma Databricks.'
        """
    )


# ============================================================
# CRIAÇÃO: SOURCE_OBJECT
# ============================================================

def create_source_object(
    spark: SparkSession,
    control_schema: str,
):
    """
    Cria o cadastro dos objetos existentes nas origens.

    IMPORTANTE:
    CHECK constraints não são declaradas aqui.
    Elas são adicionadas posteriormente com ALTER TABLE.
    """

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {control_schema}.source_object (

            source_object_key STRING NOT NULL,
            source_system_key STRING NOT NULL,

            source_schema STRING NOT NULL,
            source_table STRING NOT NULL,
            source_fqn STRING NOT NULL,

            source_key_columns ARRAY<STRING>,
            source_key_origin STRING NOT NULL,

            landing_catalog STRING NOT NULL,
            landing_schema STRING NOT NULL,
            landing_volume STRING NOT NULL,
            landing_relative_path STRING NOT NULL,

            description STRING,
            enabled BOOLEAN NOT NULL,

            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL,

            CONSTRAINT pk_source_object
                PRIMARY KEY (source_object_key) NOT ENFORCED
        )
        USING DELTA
        COMMENT
            'Cadastro dos objetos de dados disponíveis em cada sistema '
            'de origem, suas chaves de identificação e respectivos '
            'destinos físicos na Landing Zone.'
        """
    )


# ============================================================
# CHECK CONSTRAINT: SOURCE_KEY_ORIGIN
# ============================================================

def check_constraint_exists(
    spark: SparkSession,
    table_fqn: str,
    constraint_name: str,
) -> bool:
    """
    Verifica se uma CHECK constraint Delta já está registrada.

    CHECK constraints são armazenadas como propriedade Delta:

        delta.constraints.<nome_da_constraint>
    """

    expected_property = (
        f"delta.constraints.{constraint_name}".lower()
    )

    properties = spark.sql(
        f"""
        SHOW TBLPROPERTIES {table_fqn}
        """
    ).collect()

    for row in properties:

        property_key = row["key"]

        if (
            property_key
            and property_key.lower() == expected_property
        ):
            return True

    return False


def ensure_source_key_origin_check_constraint(
    spark: SparkSession,
    catalog: str,
):
    """
    Garante os valores permitidos em source_key_origin.

    Valores permitidos:

        DECLARED
        INFERRED
        CONFIGURED
        NONE

    No Databricks a CHECK constraint é adicionada após
    CREATE TABLE utilizando ALTER TABLE ADD CONSTRAINT.
    """

    table_fqn = (
        f"`{catalog}`."
        f"`{CONTROL_SCHEMA_NAME}`."
        f"`source_object`"
    )

    constraint_name = SOURCE_KEY_ORIGIN_CONSTRAINT

    if check_constraint_exists(
        spark=spark,
        table_fqn=table_fqn,
        constraint_name=constraint_name,
    ):
        print(
            f"Constraint {constraint_name} já existe."
        )
        return

    allowed_values = ", ".join(
        f"'{value}'"
        for value in SOURCE_KEY_ORIGINS
    )

    spark.sql(
        f"""
        ALTER TABLE {table_fqn}
        ADD CONSTRAINT {constraint_name}
        CHECK (
            source_key_origin IN (
                {allowed_values}
            )
        )
        """
    )

    print(
        f"Constraint {constraint_name} criada com sucesso."
    )


# ============================================================
# CRIAÇÃO: INGESTION_POLICY
# ============================================================

def create_ingestion_policy(
    spark: SparkSession,
    control_schema: str,
):
    """
    Cria as políticas declarativas de ingestão.
    """

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {control_schema}.ingestion_policy (

            source_object_key STRING NOT NULL,

            bootstrap_mode STRING NOT NULL,
            steady_state_mode STRING NOT NULL,

            incremental_column STRING,
            watermark_data_type STRING,

            lookback_days INT,

            schedule_group STRING,

            landing_format STRING NOT NULL,

            retry_enabled BOOLEAN NOT NULL,
            max_retries INT NOT NULL,
            retry_interval_seconds INT NOT NULL,

            manual_reprocessing_enabled BOOLEAN NOT NULL,
            full_refresh_enabled BOOLEAN NOT NULL,

            enabled BOOLEAN NOT NULL,

            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL,

            CONSTRAINT pk_ingestion_policy
                PRIMARY KEY (source_object_key) NOT ENFORCED
        )
        USING DELTA
        COMMENT
            'Políticas declarativas de ingestão por objeto, incluindo '
            'estratégia de carga, incrementalidade, watermark, lookback, '
            'retries e possibilidades de reprocessamento.'
        """
    )


# ============================================================
# DOCUMENTAÇÃO DAS TABELAS
# ============================================================

def get_table_comments() -> dict:
    """
    Retorna os comentários oficiais das tabelas Controller.
    """

    return {

        "source_system": (
            "Cadastro dos sistemas de origem disponíveis para ingestão. "
            "Representa a origem física ou lógica e sua integração "
            "governada com a plataforma Databricks."
        ),

        "source_object": (
            "Cadastro dos objetos de dados disponíveis em cada sistema "
            "de origem, suas chaves de identificação e respectivos "
            "destinos físicos na Landing Zone."
        ),

        "ingestion_policy": (
            "Políticas declarativas de ingestão por objeto, incluindo "
            "estratégia de carga, incrementalidade, watermark, lookback, "
            "retries e possibilidades de reprocessamento."
        ),
    }


# ============================================================
# DOCUMENTAÇÃO DAS COLUNAS
# ============================================================

def get_column_comments() -> dict:
    """
    Retorna os comentários oficiais das colunas Controller.
    """

    return {

        # ====================================================
        # SOURCE_SYSTEM
        # ====================================================

        "source_system": {

            "source_system_key": (
                "Identificador técnico, determinístico e estável "
                "do sistema de origem dentro da plataforma."
            ),

            "source_type": (
                "Tipo tecnológico da origem, por exemplo "
                "POSTGRESQL, MYSQL, SQLSERVER, API, SFTP ou FILE."
            ),

            "connection_name": (
                "Nome da conexão registrada no Unity Catalog "
                "utilizada para acessar o sistema de origem."
            ),

            "foreign_catalog": (
                "Nome do Foreign Catalog do Unity Catalog que representa "
                "de forma governada o sistema de origem, quando aplicável."
            ),

            "description": (
                "Descrição funcional e técnica do sistema de origem."
            ),

            "owner": (
                "Responsável técnico ou funcional pelo sistema "
                "ou integração de origem."
            ),

            "enabled": (
                "Indica se o sistema de origem está habilitado para "
                "participação nos processos automatizados de ingestão."
            ),

            "created_at": (
                "Timestamp de criação do registro de configuração "
                "no Controller."
            ),

            "updated_at": (
                "Timestamp da última alteração do registro de "
                "configuração no Controller."
            ),
        },

        # ====================================================
        # SOURCE_OBJECT
        # ====================================================

        "source_object": {

            "source_object_key": (
                "Identificador técnico, determinístico e estável "
                "do objeto de origem dentro da plataforma."
            ),

            "source_system_key": (
                "Identificador do sistema de origem ao qual "
                "o objeto pertence."
            ),

            "source_schema": (
                "Schema ou namespace do objeto no sistema de origem."
            ),

            "source_table": (
                "Nome físico da tabela ou objeto no sistema de origem."
            ),

            "source_fqn": (
                "Nome totalmente qualificado utilizado pela plataforma "
                "para acessar o objeto de origem."
            ),

            "source_key_columns": (
                "Lista ordenada das colunas utilizadas pela plataforma "
                "para identificar logicamente um registro do objeto "
                "de origem."
            ),

            "source_key_origin": (
                "Origem da definição da chave utilizada pela plataforma: "
                "DECLARED quando declarada formalmente na origem, "
                "INFERRED quando inferida tecnicamente, "
                "CONFIGURED quando definida explicitamente pela plataforma "
                "ou NONE quando não existe chave disponível."
            ),

            "landing_catalog": (
                "Catálogo Unity Catalog onde está localizada "
                "a Landing Zone de destino."
            ),

            "landing_schema": (
                "Schema Unity Catalog responsável pela Landing Zone "
                "do objeto."
            ),

            "landing_volume": (
                "Volume Unity Catalog utilizado para persistência "
                "física dos dados recebidos."
            ),

            "landing_relative_path": (
                "Caminho relativo dentro do volume da Landing "
                "reservado ao objeto de origem."
            ),

            "description": (
                "Descrição técnica e funcional do objeto de origem."
            ),

            "enabled": (
                "Indica se o objeto está habilitado para processamento "
                "pelos pipelines de ingestão."
            ),

            "created_at": (
                "Timestamp de criação do cadastro do objeto "
                "no Controller."
            ),

            "updated_at": (
                "Timestamp da última alteração do cadastro do objeto "
                "no Controller."
            ),
        },

        # ====================================================
        # INGESTION_POLICY
        # ====================================================

        "ingestion_policy": {

            "source_object_key": (
                "Identificador do objeto de origem ao qual esta "
                "política de ingestão se aplica."
            ),

            "bootstrap_mode": (
                "Estratégia utilizada na primeira carga ou reconstrução "
                "inicial do objeto, por exemplo FULL."
            ),

            "steady_state_mode": (
                "Estratégia utilizada nas execuções rotineiras após "
                "a carga inicial, por exemplo INCREMENTAL ou FULL."
            ),

            "incremental_column": (
                "Coluna da origem utilizada para determinar "
                "incrementalidade e evolução do watermark, "
                "quando aplicável."
            ),

            "watermark_data_type": (
                "Tipo lógico do valor utilizado como watermark, "
                "por exemplo TIMESTAMP, DATE ou NUMERIC."
            ),

            "lookback_days": (
                "Quantidade de dias retroativos acrescentados à janela "
                "incremental para capturar alterações ou dados atrasados."
            ),

            "schedule_group": (
                "Agrupamento lógico utilizado para associar objetos "
                "a uma determinada frequência ou agenda de processamento."
            ),

            "landing_format": (
                "Formato físico utilizado para persistência do material "
                "recebido na Landing Zone."
            ),

            "retry_enabled": (
                "Indica se falhas transitórias de ingestão podem "
                "provocar novas tentativas automáticas."
            ),

            "max_retries": (
                "Quantidade máxima de novas tentativas automáticas "
                "permitidas para uma execução com falha recuperável."
            ),

            "retry_interval_seconds": (
                "Intervalo em segundos entre tentativas automáticas "
                "sucessivas."
            ),

            "manual_reprocessing_enabled": (
                "Indica se o objeto permite reprocessamentos corretivos "
                "disparados manualmente."
            ),

            "full_refresh_enabled": (
                "Indica se o objeto pode ser submetido a uma "
                "reconstrução completa controlada."
            ),

            "enabled": (
                "Indica se a política está habilitada e disponível "
                "para uso pelo processo de ingestão."
            ),

            "created_at": (
                "Timestamp de criação da política de ingestão "
                "no Controller."
            ),

            "updated_at": (
                "Timestamp da última alteração da política de ingestão "
                "no Controller."
            ),
        },
    }


# ============================================================
# APLICAÇÃO DOS COMENTÁRIOS
# ============================================================

def apply_comments(
    spark: SparkSession,
    control_schema: str,
):
    """
    Garante comentários nas tabelas e colunas.

    A rotina é executada em todas as execuções para permitir
    evolução da documentação sem necessidade de recriar tabelas.
    """

    table_comments = get_table_comments()
    column_comments = get_column_comments()

    # --------------------------------------------------------
    # Comentários das tabelas
    # --------------------------------------------------------

    for table_name, table_comment in table_comments.items():

        escaped_comment = escape_sql_literal(
            table_comment
        )

        spark.sql(
            f"""
            COMMENT ON TABLE
                {control_schema}.`{table_name}`
            IS
                '{escaped_comment}'
            """
        )

    # --------------------------------------------------------
    # Comentários das colunas
    # --------------------------------------------------------

    for table_name, columns in column_comments.items():

        for column_name, column_comment in columns.items():

            escaped_comment = escape_sql_literal(
                column_comment
            )

            spark.sql(
                f"""
                COMMENT ON COLUMN
                    {control_schema}.`{table_name}`.`{column_name}`
                IS
                    '{escaped_comment}'
                """
            )


# ============================================================
# VALIDAÇÃO DO CONTRATO FINAL
# ============================================================

def validate_source_object_contract(
    spark: SparkSession,
    catalog: str,
):
    """
    Valida se source_object terminou o bootstrap com
    o contrato esperado.
    """

    expected_columns = {
        "source_object_key",
        "source_system_key",
        "source_schema",
        "source_table",
        "source_fqn",
        "source_key_columns",
        "source_key_origin",
        "landing_catalog",
        "landing_schema",
        "landing_volume",
        "landing_relative_path",
        "description",
        "enabled",
        "created_at",
        "updated_at",
    }

    columns = get_table_columns(
        spark=spark,
        catalog=catalog,
        schema=CONTROL_SCHEMA_NAME,
        table="source_object",
    )

    missing_columns = (
        expected_columns - columns
    )

    unexpected_old_columns = {
        "source_primary_key"
    }.intersection(columns)

    if missing_columns:
        raise RuntimeError(
            "Contrato inválido em source_object. "
            f"Colunas ausentes: {sorted(missing_columns)}"
        )

    if unexpected_old_columns:
        raise RuntimeError(
            "Contrato antigo ainda presente em source_object. "
            f"Colunas antigas: {sorted(unexpected_old_columns)}"
        )

    print(
        "Contrato de source_object validado com sucesso."
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

    control_schema = (
        f"`{catalog}`."
        f"`{CONTROL_SCHEMA_NAME}`"
    )

    print(
        "============================================================"
    )
    print(
        f"Iniciando bootstrap do Controller em "
        f"{catalog}.{CONTROL_SCHEMA_NAME}"
    )
    print(
        "============================================================"
    )

    # --------------------------------------------------------
    # 1. Migração defensiva
    # --------------------------------------------------------

    migrate_source_object_if_required(
        spark=spark,
        catalog=catalog,
    )

    # --------------------------------------------------------
    # 2. Criação de source_system
    # --------------------------------------------------------

    create_source_system(
        spark=spark,
        control_schema=control_schema,
    )

    print(
        "Tabela source_system criada/validada."
    )

    # --------------------------------------------------------
    # 3. Criação de source_object
    # --------------------------------------------------------

    create_source_object(
        spark=spark,
        control_schema=control_schema,
    )

    print(
        "Tabela source_object criada/validada."
    )

    # --------------------------------------------------------
    # 4. CHECK constraint de source_object
    # --------------------------------------------------------

    ensure_source_key_origin_check_constraint(
        spark=spark,
        catalog=catalog,
    )

    # --------------------------------------------------------
    # 5. Criação de ingestion_policy
    # --------------------------------------------------------

    create_ingestion_policy(
        spark=spark,
        control_schema=control_schema,
    )

    print(
        "Tabela ingestion_policy criada/validada."
    )

    # --------------------------------------------------------
    # 6. Governança e documentação
    # --------------------------------------------------------

    apply_comments(
        spark=spark,
        control_schema=control_schema,
    )

    print(
        "Comentários das tabelas e colunas aplicados."
    )

    # --------------------------------------------------------
    # 7. Validação final do contrato
    # --------------------------------------------------------

    validate_source_object_contract(
        spark=spark,
        catalog=catalog,
    )

    print(
        "============================================================"
    )
    print(
        f"Controller core criado/validado com sucesso em "
        f"{catalog}.{CONTROL_SCHEMA_NAME}"
    )
    print(
        "============================================================"
    )


if __name__ == "__main__":
    main()
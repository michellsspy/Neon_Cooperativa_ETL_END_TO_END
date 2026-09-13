import argparse
import re

from pyspark.sql import SparkSession


# ============================================================
# CONSTANTES
# ============================================================

CONTROL_SCHEMA = "control"

INGESTION_RUN_TABLE = "ingestion_run"

INGESTION_RUN_OBJECT_TABLE = "ingestion_run_object"


# ============================================================
# DOMÍNIOS CONTROLADOS
# ============================================================

RUN_TRIGGER_TYPES = (
    "SCHEDULED",
    "MANUAL",
    "REPROCESS",
    "BACKFILL",
    "RETRY",
)

RUN_STATUSES = (
    "CREATED",
    "RUNNING",
    "SUCCEEDED",
    "PARTIAL",
    "FAILED",
    "CANCELLED",
)

RUN_OBJECT_LOAD_MODES = (
    "FULL",
    "INCREMENTAL",
)

RUN_OBJECT_STATUSES = (
    "PENDING",
    "RUNNING",
    "MATERIALIZED",
    "RECONCILED",
    "SUCCEEDED",
    "FAILED",
    "SKIPPED",
)


# ============================================================
# ARGUMENTOS
# ============================================================

def parse_args():
    """
    Lê os argumentos utilizados pelo Job de bootstrap.
    """

    parser = argparse.ArgumentParser(
        description=(
            "Cria o modelo operacional de auditoria "
            "das execuções de ingestão."
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


def escape_sql_literal(value: str) -> str:
    """
    Escapa aspas simples para utilização
    em literais SQL.
    """

    return value.replace(
        "'",
        "''",
    )


def sql_string_list(values) -> str:
    """
    Converte uma coleção Python para lista
    de literais STRING SQL.
    """

    return ", ".join(
        f"'{escape_sql_literal(value)}'"
        for value in values
    )


# ============================================================
# PRÉ-REQUISITOS
# ============================================================

def validate_prerequisites(
    spark: SparkSession,
    catalog: str,
):
    """
    Garante que as estruturas necessárias já existem
    antes da criação do ledger operacional.
    """

    required_tables = (
        "source_object",
        "ingestion_policy",
        "ingestion_state",
    )

    missing_tables = []

    for table_name in required_tables:

        result = spark.sql(
            f"""
            SELECT
                COUNT(*) AS qtd

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
# INGESTION_RUN
# ============================================================

def create_ingestion_run(
    spark: SparkSession,
    catalog: str,
):
    """
    Cria o envelope de execução da ingestão.

    Uma linha representa uma execução lógica da plataforma,
    podendo conter vários objetos de origem.
    """

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS
            `{catalog}`.`{CONTROL_SCHEMA}`.`{INGESTION_RUN_TABLE}`
        (
            run_id STRING NOT NULL,

            trigger_type STRING NOT NULL,

            trigger_reference STRING,

            schedule_group STRING,

            requested_by STRING,

            orchestrator_job_id STRING,

            orchestrator_run_id STRING,

            status STRING NOT NULL,

            started_at TIMESTAMP NOT NULL,

            completed_at TIMESTAMP,

            created_at TIMESTAMP NOT NULL,

            updated_at TIMESTAMP NOT NULL,

            CONSTRAINT pk_ingestion_run
                PRIMARY KEY (run_id)
                NOT ENFORCED
        )

        USING DELTA

        COMMENT
            'Ledger operacional das execuções de ingestão. '
            'Cada registro representa uma execução lógica da '
            'plataforma, independente dos identificadores do '
            'orquestrador Databricks.'
        """
    )

    print(
        "Tabela ingestion_run criada/validada."
    )


# ============================================================
# INGESTION_RUN_OBJECT
# ============================================================

def create_ingestion_run_object(
    spark: SparkSession,
    catalog: str,
):
    """
    Cria o ledger operacional de execução por objeto.

    Uma linha representa uma tentativa específica de processar
    um source_object dentro de uma execução ingestion_run.
    """

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS
            `{catalog}`.`{CONTROL_SCHEMA}`.`{INGESTION_RUN_OBJECT_TABLE}`
        (
            run_object_id STRING NOT NULL,

            run_id STRING NOT NULL,

            source_object_key STRING NOT NULL,

            attempt_number INT NOT NULL,

            reprocess_of_run_object_id STRING,

            load_mode STRING NOT NULL,

            status STRING NOT NULL,

            watermark_column STRING,

            watermark_lower_bound STRING,

            watermark_upper_bound STRING,

            lookback_days_applied INT,

            source_row_count BIGINT,

            landing_row_count BIGINT,

            landing_data_path STRING,

            manifest_path STRING,

            state_version_before BIGINT,

            state_version_after BIGINT,

            state_committed BOOLEAN NOT NULL,

            error_class STRING,

            error_message STRING,

            started_at TIMESTAMP,

            completed_at TIMESTAMP,

            created_at TIMESTAMP NOT NULL,

            updated_at TIMESTAMP NOT NULL,

            CONSTRAINT pk_ingestion_run_object
                PRIMARY KEY (run_object_id)
                NOT ENFORCED
        )

        USING DELTA

        COMMENT
            'Ledger operacional da execução de ingestão por objeto. '
            'Registra tentativa, modo de carga, janela incremental, '
            'materialização na Landing, contagens, commit de estado '
            'e informações de falha.'
        """
    )

    print(
        "Tabela ingestion_run_object criada/validada."
    )


# ============================================================
# CONTRATO ESPERADO DAS TABELAS
# ============================================================

def get_expected_columns():
    """
    Define o contrato mínimo esperado das duas tabelas.
    """

    return {

        INGESTION_RUN_TABLE: {
            "run_id",
            "trigger_type",
            "trigger_reference",
            "schedule_group",
            "requested_by",
            "orchestrator_job_id",
            "orchestrator_run_id",
            "status",
            "started_at",
            "completed_at",
            "created_at",
            "updated_at",
        },

        INGESTION_RUN_OBJECT_TABLE: {
            "run_object_id",
            "run_id",
            "source_object_key",
            "attempt_number",
            "reprocess_of_run_object_id",
            "load_mode",
            "status",
            "watermark_column",
            "watermark_lower_bound",
            "watermark_upper_bound",
            "lookback_days_applied",
            "source_row_count",
            "landing_row_count",
            "landing_data_path",
            "manifest_path",
            "state_version_before",
            "state_version_after",
            "state_committed",
            "error_class",
            "error_message",
            "started_at",
            "completed_at",
            "created_at",
            "updated_at",
        },
    }


def validate_table_contracts(
    spark: SparkSession,
    catalog: str,
):
    """
    Confirma que todas as colunas necessárias existem.

    Colunas adicionais são toleradas para permitir evolução
    futura do modelo sem tornar o bootstrap destrutivo.
    """

    expected_contract = (
        get_expected_columns()
    )

    for table_name, expected_columns in (
        expected_contract.items()
    ):

        rows = spark.sql(
            f"""
            SELECT
                column_name

            FROM
                `{catalog}`.information_schema.columns

            WHERE
                table_schema = '{CONTROL_SCHEMA}'

                AND table_name = '{table_name}'
            """
        ).collect()

        actual_columns = {
            row["column_name"]
            for row in rows
        }

        missing_columns = (
            expected_columns
            - actual_columns
        )

        if missing_columns:

            raise RuntimeError(
                f"Contrato inválido em {table_name}. "
                f"Colunas ausentes: "
                f"{sorted(missing_columns)}"
            )

    print(
        "Contratos estruturais das tabelas "
        "de execução validados."
    )


# ============================================================
# FOREIGN KEYS
# ============================================================

def key_constraint_exists(
    spark: SparkSession,
    catalog: str,
    table_name: str,
    constraint_name: str,
) -> bool:
    """
    Verifica existência de PRIMARY/FOREIGN KEY
    através do information_schema.
    """

    constraint_literal = (
        escape_sql_literal(
            constraint_name
        )
    )

    table_literal = (
        escape_sql_literal(
            table_name
        )
    )

    result = spark.sql(
        f"""
        SELECT
            COUNT(*) AS qtd

        FROM
            `{catalog}`.information_schema.table_constraints

        WHERE
            table_schema = '{CONTROL_SCHEMA}'

            AND table_name = '{table_literal}'

            AND constraint_name = '{constraint_literal}'
        """
    ).first()

    return result["qtd"] > 0


def ensure_foreign_keys(
    spark: SparkSession,
    catalog: str,
):
    """
    Adiciona relacionamentos informacionais entre:

        ingestion_run_object -> ingestion_run
        ingestion_run_object -> source_object

    As FKs documentam o modelo, mas a aplicação continuará
    responsável pela integridade operacional.
    """

    child_table = (
        f"`{catalog}`."
        f"`{CONTROL_SCHEMA}`."
        f"`{INGESTION_RUN_OBJECT_TABLE}`"
    )

    # --------------------------------------------------------
    # run_id -> ingestion_run.run_id
    # --------------------------------------------------------

    constraint_name = (
        "fk_ingestion_run_object_run"
    )

    if not key_constraint_exists(
        spark=spark,
        catalog=catalog,
        table_name=INGESTION_RUN_OBJECT_TABLE,
        constraint_name=constraint_name,
    ):

        spark.sql(
            f"""
            ALTER TABLE {child_table}

            ADD CONSTRAINT {constraint_name}

            FOREIGN KEY (run_id)

            REFERENCES
                `{catalog}`.`{CONTROL_SCHEMA}`.`{INGESTION_RUN_TABLE}`
                (run_id)

            NOT ENFORCED
            """
        )

        print(
            f"Constraint {constraint_name} criada."
        )

    else:

        print(
            f"Constraint {constraint_name} já existe."
        )

    # --------------------------------------------------------
    # source_object_key -> source_object.source_object_key
    # --------------------------------------------------------

    constraint_name = (
        "fk_ingestion_run_object_source_object"
    )

    if not key_constraint_exists(
        spark=spark,
        catalog=catalog,
        table_name=INGESTION_RUN_OBJECT_TABLE,
        constraint_name=constraint_name,
    ):

        spark.sql(
            f"""
            ALTER TABLE {child_table}

            ADD CONSTRAINT {constraint_name}

            FOREIGN KEY (source_object_key)

            REFERENCES
                `{catalog}`.`{CONTROL_SCHEMA}`.`source_object`
                (source_object_key)

            NOT ENFORCED
            """
        )

        print(
            f"Constraint {constraint_name} criada."
        )

    else:

        print(
            f"Constraint {constraint_name} já existe."
        )


# ============================================================
# CHECK CONSTRAINTS
# ============================================================

def check_constraint_exists(
    spark: SparkSession,
    table_fqn: str,
    constraint_name: str,
) -> bool:
    """
    CHECK constraints Delta são persistidas como
    propriedades da tabela.
    """

    expected_property = (
        f"delta.constraints.{constraint_name}"
        .lower()
    )

    rows = spark.sql(
        f"""
        SHOW TBLPROPERTIES {table_fqn}
        """
    ).collect()

    for row in rows:

        property_key = row["key"]

        if (
            property_key
            and property_key.lower()
            == expected_property
        ):
            return True

    return False


def add_check_constraint_if_missing(
    spark: SparkSession,
    table_fqn: str,
    constraint_name: str,
    condition: str,
):
    """
    Adiciona CHECK constraint somente quando ainda
    não estiver registrada.
    """

    if check_constraint_exists(
        spark=spark,
        table_fqn=table_fqn,
        constraint_name=constraint_name,
    ):

        print(
            f"Constraint {constraint_name} já existe."
        )

        return

    spark.sql(
        f"""
        ALTER TABLE {table_fqn}

        ADD CONSTRAINT {constraint_name}

        CHECK (
            {condition}
        )
        """
    )

    print(
        f"Constraint {constraint_name} criada."
    )


def ensure_check_constraints(
    spark: SparkSession,
    catalog: str,
):
    """
    Garante domínios operacionais mínimos.

    CHECK constraints são adicionadas com ALTER TABLE,
    conforme o contrato suportado pelo Databricks.
    """

    run_table_fqn = (
        f"`{catalog}`."
        f"`{CONTROL_SCHEMA}`."
        f"`{INGESTION_RUN_TABLE}`"
    )

    run_object_table_fqn = (
        f"`{catalog}`."
        f"`{CONTROL_SCHEMA}`."
        f"`{INGESTION_RUN_OBJECT_TABLE}`"
    )

    # --------------------------------------------------------
    # ingestion_run.trigger_type
    # --------------------------------------------------------

    add_check_constraint_if_missing(
        spark=spark,
        table_fqn=run_table_fqn,
        constraint_name="ck_ingestion_run_trigger_type",
        condition=(
            "trigger_type IN "
            f"({sql_string_list(RUN_TRIGGER_TYPES)})"
        ),
    )

    # --------------------------------------------------------
    # ingestion_run.status
    # --------------------------------------------------------

    add_check_constraint_if_missing(
        spark=spark,
        table_fqn=run_table_fqn,
        constraint_name="ck_ingestion_run_status",
        condition=(
            "status IN "
            f"({sql_string_list(RUN_STATUSES)})"
        ),
    )

    # --------------------------------------------------------
    # ingestion_run_object.load_mode
    # --------------------------------------------------------

    add_check_constraint_if_missing(
        spark=spark,
        table_fqn=run_object_table_fqn,
        constraint_name="ck_ingestion_run_object_load_mode",
        condition=(
            "load_mode IN "
            f"({sql_string_list(RUN_OBJECT_LOAD_MODES)})"
        ),
    )

    # --------------------------------------------------------
    # ingestion_run_object.status
    # --------------------------------------------------------

    add_check_constraint_if_missing(
        spark=spark,
        table_fqn=run_object_table_fqn,
        constraint_name="ck_ingestion_run_object_status",
        condition=(
            "status IN "
            f"({sql_string_list(RUN_OBJECT_STATUSES)})"
        ),
    )

    # --------------------------------------------------------
    # attempt_number
    # --------------------------------------------------------

    add_check_constraint_if_missing(
        spark=spark,
        table_fqn=run_object_table_fqn,
        constraint_name="ck_ingestion_run_object_attempt",
        condition=(
            "attempt_number >= 1"
        ),
    )

    # --------------------------------------------------------
    # lookback
    # --------------------------------------------------------

    add_check_constraint_if_missing(
        spark=spark,
        table_fqn=run_object_table_fqn,
        constraint_name="ck_ingestion_run_object_lookback",
        condition=(
            "lookback_days_applied IS NULL "
            "OR lookback_days_applied >= 0"
        ),
    )

    # --------------------------------------------------------
    # source_row_count
    # --------------------------------------------------------

    add_check_constraint_if_missing(
        spark=spark,
        table_fqn=run_object_table_fqn,
        constraint_name="ck_ingestion_run_object_source_rows",
        condition=(
            "source_row_count IS NULL "
            "OR source_row_count >= 0"
        ),
    )

    # --------------------------------------------------------
    # landing_row_count
    # --------------------------------------------------------

    add_check_constraint_if_missing(
        spark=spark,
        table_fqn=run_object_table_fqn,
        constraint_name="ck_ingestion_run_object_landing_rows",
        condition=(
            "landing_row_count IS NULL "
            "OR landing_row_count >= 0"
        ),
    )

    # --------------------------------------------------------
    # state versions
    # --------------------------------------------------------

    add_check_constraint_if_missing(
        spark=spark,
        table_fqn=run_object_table_fqn,
        constraint_name="ck_ingestion_run_object_state_version_before",
        condition=(
            "state_version_before IS NULL "
            "OR state_version_before >= 0"
        ),
    )

    add_check_constraint_if_missing(
        spark=spark,
        table_fqn=run_object_table_fqn,
        constraint_name="ck_ingestion_run_object_state_version_after",
        condition=(
            "state_version_after IS NULL "
            "OR state_version_after >= 0"
        ),
    )


# ============================================================
# DOCUMENTAÇÃO
# ============================================================

def apply_comments(
    spark: SparkSession,
    catalog: str,
):
    """
    Mantém comentários atualizáveis mesmo quando
    as tabelas já existirem.
    """

    table_comments = {

        INGESTION_RUN_TABLE: (
            "Ledger operacional das execuções de ingestão. "
            "Cada registro representa uma execução lógica "
            "da plataforma, independente dos identificadores "
            "do orquestrador Databricks."
        ),

        INGESTION_RUN_OBJECT_TABLE: (
            "Ledger operacional da execução de ingestão por objeto. "
            "Registra tentativa, modo de carga, janela incremental, "
            "materialização na Landing, contagens, commit de estado "
            "e informações de falha."
        ),
    }

    column_comments = {

        INGESTION_RUN_TABLE: {

            "run_id": (
                "Identificador único e estável da execução lógica "
                "gerado pela plataforma de ingestão."
            ),

            "trigger_type": (
                "Origem operacional do disparo da execução, como "
                "SCHEDULED, MANUAL, REPROCESS, BACKFILL ou RETRY."
            ),

            "trigger_reference": (
                "Referência opcional associada ao disparo, como "
                "identificador de execução original, solicitação "
                "manual ou contexto de backfill."
            ),

            "schedule_group": (
                "Grupo lógico de agendamento selecionado "
                "para a execução."
            ),

            "requested_by": (
                "Usuário, service principal ou identidade que "
                "solicitou ou originou a execução."
            ),

            "orchestrator_job_id": (
                "Identificador do Job no orquestrador Databricks, "
                "quando disponível."
            ),

            "orchestrator_run_id": (
                "Identificador da execução no orquestrador Databricks, "
                "mantido separadamente do run_id da plataforma."
            ),

            "status": (
                "Estado operacional consolidado da execução lógica."
            ),

            "started_at": (
                "Timestamp de início efetivo da execução."
            ),

            "completed_at": (
                "Timestamp de término da execução. Permanece NULL "
                "enquanto a execução não estiver finalizada."
            ),

            "created_at": (
                "Timestamp de criação do registro de execução."
            ),

            "updated_at": (
                "Timestamp da última alteração do registro "
                "de execução."
            ),
        },

        INGESTION_RUN_OBJECT_TABLE: {

            "run_object_id": (
                "Identificador único da tentativa de processamento "
                "de um objeto dentro de uma execução."
            ),

            "run_id": (
                "Identificador da execução lógica à qual "
                "esta tentativa pertence."
            ),

            "source_object_key": (
                "Identificador do objeto de origem processado."
            ),

            "attempt_number": (
                "Número ordinal da tentativa do objeto dentro "
                "da execução, iniciando em 1."
            ),

            "reprocess_of_run_object_id": (
                "Identificador de uma execução anterior do objeto "
                "que está sendo reprocessada, quando aplicável."
            ),

            "load_mode": (
                "Estratégia física utilizada nesta tentativa: "
                "FULL ou INCREMENTAL."
            ),

            "status": (
                "Estado operacional da tentativa de processamento "
                "do objeto."
            ),

            "watermark_column": (
                "Coluna utilizada como watermark nesta execução, "
                "quando aplicável."
            ),

            "watermark_lower_bound": (
                "Limite inferior efetivamente utilizado na janela "
                "incremental, já considerando o lookback."
            ),

            "watermark_upper_bound": (
                "Limite superior congelado para a execução. "
                "Após sucesso e reconciliação poderá tornar-se "
                "o novo watermark confirmado."
            ),

            "lookback_days_applied": (
                "Quantidade de dias de sobreposição efetivamente "
                "aplicada à janela incremental."
            ),

            "source_row_count": (
                "Quantidade de registros obtidos da origem "
                "pela tentativa."
            ),

            "landing_row_count": (
                "Quantidade de registros confirmados fisicamente "
                "na Landing pela tentativa."
            ),

            "landing_data_path": (
                "Caminho físico imutável onde os dados da tentativa "
                "foram persistidos na Landing."
            ),

            "manifest_path": (
                "Caminho do manifesto associado à materialização "
                "da tentativa."
            ),

            "state_version_before": (
                "Versão do ingestion_state observada antes do "
                "commit operacional desta tentativa."
            ),

            "state_version_after": (
                "Versão do ingestion_state após commit bem-sucedido. "
                "Permanece NULL enquanto nenhum commit for confirmado."
            ),

            "state_committed": (
                "Indica se o resultado desta tentativa foi efetivamente "
                "utilizado para avançar o ingestion_state."
            ),

            "error_class": (
                "Classe ou categoria técnica da falha, "
                "quando a tentativa não for bem-sucedida."
            ),

            "error_message": (
                "Mensagem técnica associada à falha da tentativa."
            ),

            "started_at": (
                "Timestamp de início do processamento do objeto."
            ),

            "completed_at": (
                "Timestamp de término do processamento do objeto."
            ),

            "created_at": (
                "Timestamp de criação do registro operacional."
            ),

            "updated_at": (
                "Timestamp da última alteração do registro operacional."
            ),
        },
    }

    # --------------------------------------------------------
    # Comentários das tabelas
    # --------------------------------------------------------

    for table_name, comment in (
        table_comments.items()
    ):

        escaped_comment = (
            escape_sql_literal(
                comment
            )
        )

        spark.sql(
            f"""
            COMMENT ON TABLE
                `{catalog}`.`{CONTROL_SCHEMA}`.`{table_name}`

            IS
                '{escaped_comment}'
            """
        )

    # --------------------------------------------------------
    # Comentários das colunas
    # --------------------------------------------------------

    for table_name, columns in (
        column_comments.items()
    ):

        for column_name, comment in (
            columns.items()
        ):

            escaped_comment = (
                escape_sql_literal(
                    comment
                )
            )

            spark.sql(
                f"""
                COMMENT ON COLUMN
                    `{catalog}`.`{CONTROL_SCHEMA}`.`{table_name}`.`{column_name}`

                IS
                    '{escaped_comment}'
                """
            )

    print(
        "Comentários das tabelas de execução aplicados."
    )


# ============================================================
# VALIDAÇÃO DOS RELACIONAMENTOS
# ============================================================

def validate_foreign_keys(
    spark: SparkSession,
    catalog: str,
):
    """
    Confirma que as duas FKs informacionais esperadas
    foram registradas no Unity Catalog.
    """

    expected_constraints = {
        "fk_ingestion_run_object_run",
        "fk_ingestion_run_object_source_object",
    }

    rows = spark.sql(
        f"""
        SELECT
            constraint_name

        FROM
            `{catalog}`.information_schema.table_constraints

        WHERE
            table_schema = '{CONTROL_SCHEMA}'

            AND table_name =
                '{INGESTION_RUN_OBJECT_TABLE}'

            AND constraint_type =
                'FOREIGN KEY'
        """
    ).collect()

    actual_constraints = {
        row["constraint_name"]
        for row in rows
    }

    missing_constraints = (
        expected_constraints
        - actual_constraints
    )

    if missing_constraints:

        raise RuntimeError(
            "Foreign Keys ausentes em "
            "ingestion_run_object: "
            f"{sorted(missing_constraints)}"
        )

    print(
        "Relacionamentos informacionais validados."
    )


# ============================================================
# RESUMO
# ============================================================

def print_summary(
    spark: SparkSession,
    catalog: str,
):
    """
    Mostra o estado atual das tabelas.

    Na primeira execução esperamos zero registros.
    Em execuções futuras o bootstrap deve preservar
    integralmente o histórico já existente.
    """

    run_count = spark.sql(
        f"""
        SELECT
            COUNT(*) AS qtd

        FROM
            `{catalog}`.`{CONTROL_SCHEMA}`.`{INGESTION_RUN_TABLE}`
        """
    ).first()["qtd"]

    run_object_count = spark.sql(
        f"""
        SELECT
            COUNT(*) AS qtd

        FROM
            `{catalog}`.`{CONTROL_SCHEMA}`.`{INGESTION_RUN_OBJECT_TABLE}`
        """
    ).first()["qtd"]

    print(
        "============================================================"
    )

    print(
        "Modelo operacional de execução:"
    )

    print(
        f"  ingestion_run........: "
        f"{run_count} registro(s)"
    )

    print(
        f"  ingestion_run_object.: "
        f"{run_object_count} registro(s)"
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
        "Bootstrap do ledger operacional de ingestão"
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
    # 2. Estruturas
    # --------------------------------------------------------

    create_ingestion_run(
        spark=spark,
        catalog=catalog,
    )

    create_ingestion_run_object(
        spark=spark,
        catalog=catalog,
    )

    # --------------------------------------------------------
    # 3. Contrato estrutural
    # --------------------------------------------------------

    validate_table_contracts(
        spark=spark,
        catalog=catalog,
    )

    # --------------------------------------------------------
    # 4. Relacionamentos informacionais
    # --------------------------------------------------------

    ensure_foreign_keys(
        spark=spark,
        catalog=catalog,
    )

    # --------------------------------------------------------
    # 5. Domínios enforced
    # --------------------------------------------------------

    ensure_check_constraints(
        spark=spark,
        catalog=catalog,
    )

    # --------------------------------------------------------
    # 6. Governança / documentação
    # --------------------------------------------------------

    apply_comments(
        spark=spark,
        catalog=catalog,
    )

    # --------------------------------------------------------
    # 7. Validação final
    # --------------------------------------------------------

    validate_foreign_keys(
        spark=spark,
        catalog=catalog,
    )

    print_summary(
        spark=spark,
        catalog=catalog,
    )

    print(
        "Bootstrap do ledger operacional "
        "concluído com sucesso."
    )


if __name__ == "__main__":
    main()
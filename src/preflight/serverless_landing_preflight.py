import argparse
import re

from pyspark.sql import Observation
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# CONSTANTES
# ============================================================

LANDING_SCHEMA = "landing"

PREFLIGHT_ROOT = "_preflight/serverless_landing"


# ============================================================
# ARGUMENTOS
# ============================================================

def parse_args():

    parser = argparse.ArgumentParser(
        description=(
            "Preflight de compatibilidade entre Lakeflow Jobs "
            "Serverless, Spark Connect e Unity Catalog Volumes."
        )
    )

    parser.add_argument(
        "--catalog",
        required=True,
        help="Catálogo gerenciado do projeto.",
    )

    parser.add_argument(
        "--landing-volume",
        required=True,
        help="Volume Unity Catalog utilizado pela Landing.",
    )

    parser.add_argument(
        "--orchestrator-run-id",
        required=True,
        help="Run ID do Lakeflow Job utilizado para isolar o teste.",
    )

    return parser.parse_args()


# ============================================================
# VALIDAÇÃO
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


def validate_run_reference(value: str) -> str:
    """
    Dynamic values do Jobs normalmente são numéricos,
    mas deixamos suporte a hífen/underscore para robustez.
    """

    if not re.fullmatch(
        r"[A-Za-z0-9_-]+",
        value,
    ):
        raise ValueError(
            f"orchestrator_run_id inválido: {value}"
        )

    return value


# ============================================================
# PATHS
# ============================================================

def build_paths(
    catalog: str,
    landing_volume: str,
    orchestrator_run_id: str,
) -> dict:

    base_path = (
        f"/Volumes/"
        f"{catalog}/"
        f"{LANDING_SCHEMA}/"
        f"{landing_volume}/"
        f"{PREFLIGHT_ROOT}/"
        f"job_run_id={orchestrator_run_id}"
    )

    return {
        "base_path": base_path,
        "data_path": (
            f"{base_path}/data"
        ),
        "manifest_path": (
            f"{base_path}/manifest"
        ),
    }


# ============================================================
# DATAFRAME DE TESTE
# ============================================================

def build_test_dataframe(
    spark: SparkSession,
):

    return spark.createDataFrame(
        [
            (1, "alpha"),
            (2, "beta"),
            (3, "gamma"),
        ],
        schema=(
            "record_id LONG, "
            "record_value STRING"
        ),
    )


# ============================================================
# TESTE 1
# OBSERVATION + PARQUET + VOLUME
# ============================================================

def test_parquet_write_with_observation(
    spark: SparkSession,
    data_path: str,
):
    """
    Prova:

        Spark Connect DataFrame
        Observation
        DataFrameWriter
        mode('error')
        Parquet
        Unity Catalog Volume
    """

    source_df = build_test_dataframe(
        spark=spark,
    )

    observation = Observation(
        "serverless_landing_preflight"
    )

    observed_df = source_df.observe(
        observation,

        F.count(
            F.lit(1)
        ).alias(
            "source_row_count"
        ),
    )

    print(
        "Executando escrita Parquet com Observation..."
    )

    (
        observed_df
        .write
        .format("parquet")
        .mode("error")
        .save(data_path)
    )

    metrics = observation.get

    source_row_count = int(
        metrics["source_row_count"]
    )

    if source_row_count != 3:

        raise RuntimeError(
            "Observation retornou quantidade inesperada. "
            f"Esperado=3, encontrado={source_row_count}"
        )

    print(
        f"Observation OK: "
        f"{source_row_count} registro(s)."
    )

    return source_row_count


# ============================================================
# TESTE 2
# READ-BACK
# ============================================================

def test_parquet_read_back(
    spark: SparkSession,
    data_path: str,
    expected_count: int,
):
    """
    Faz leitura independente do material físico gravado.
    """

    landing_df = (
        spark.read
        .format("parquet")
        .load(data_path)
    )

    landing_row_count = int(
        landing_df.count()
    )

    if landing_row_count != expected_count:

        raise RuntimeError(
            "Read-back apresentou divergência. "
            f"Esperado={expected_count}, "
            f"encontrado={landing_row_count}"
        )

    ids = {
        row["record_id"]
        for row in (
            landing_df
            .select("record_id")
            .collect()
        )
    }

    if ids != {1, 2, 3}:

        raise RuntimeError(
            "Read-back apresentou conteúdo inesperado. "
            f"IDs encontrados={sorted(ids)}"
        )

    print(
        f"Read-back Parquet OK: "
        f"{landing_row_count} registro(s)."
    )

    return landing_row_count


# ============================================================
# TESTE 3
# MANIFEST JSON
# ============================================================

def test_manifest_write(
    spark: SparkSession,
    manifest_path: str,
    data_path: str,
    row_count: int,
):

    manifest_df = spark.createDataFrame(
        [
            (
                "1.0",
                "SUCCESS",
                data_path,
                row_count,
            )
        ],
        schema=(
            "manifest_version STRING, "
            "status STRING, "
            "data_path STRING, "
            "row_count LONG"
        ),
    )

    (
        manifest_df
        .write
        .format("json")
        .mode("error")
        .save(manifest_path)
    )

    manifest_read = (
        spark.read
        .format("json")
        .load(manifest_path)
    )

    manifest_count = int(
        manifest_read.count()
    )

    if manifest_count != 1:

        raise RuntimeError(
            "Manifest apresentou quantidade inesperada. "
            f"Esperado=1, encontrado={manifest_count}"
        )

    manifest_row = manifest_read.first()

    if manifest_row["status"] != "SUCCESS":

        raise RuntimeError(
            "Manifest apresentou status inesperado."
        )

    if int(
        manifest_row["row_count"]
    ) != row_count:

        raise RuntimeError(
            "Manifest apresentou row_count divergente."
        )

    print(
        "Manifest JSON OK."
    )


# ============================================================
# TESTE 4
# IMUTABILIDADE DO PATH
# ============================================================

def test_error_mode_collision(
    spark: SparkSession,
    data_path: str,
):
    """
    A segunda escrita no mesmo path deve falhar.

    Esse teste é essencial porque nosso contrato de Landing
    exige que uma tentativa nunca sobrescreva outra.
    """

    second_df = spark.createDataFrame(
        [
            (999, "should_not_be_written"),
        ],
        schema=(
            "record_id LONG, "
            "record_value STRING"
        ),
    )

    print(
        "Testando colisão com mode('error')..."
    )

    try:

        (
            second_df
            .write
            .format("parquet")
            .mode("error")
            .save(data_path)
        )

    except Exception as exc:

        print(
            "Colisão bloqueada conforme esperado."
        )

        print(
            f"  exception_class: "
            f"{exc.__class__.__name__}"
        )

        print(
            f"  message.........: "
            f"{str(exc)[:500]}"
        )

        return

    raise RuntimeError(
        "FALHA DE IMUTABILIDADE: "
        "a segunda escrita no mesmo path não falhou."
    )


# ============================================================
# VALIDAÇÃO FINAL
# ============================================================

def validate_final_state(
    spark: SparkSession,
    data_path: str,
):
    """
    Garante que o teste de colisão não modificou
    o conteúdo original.
    """

    df = (
        spark.read
        .format("parquet")
        .load(data_path)
    )

    final_count = int(
        df.count()
    )

    if final_count != 3:

        raise RuntimeError(
            "O conteúdo foi alterado após teste "
            "de colisão. "
            f"Esperado=3, encontrado={final_count}"
        )

    ids = {
        row["record_id"]
        for row in (
            df
            .select("record_id")
            .collect()
        )
    }

    if ids != {1, 2, 3}:

        raise RuntimeError(
            "O conteúdo físico foi alterado "
            "após a colisão."
        )

    print(
        "Imutabilidade física validada."
    )


# ============================================================
# MAIN
# ============================================================

def main():

    args = parse_args()

    catalog = validate_identifier(
        args.catalog
    )

    landing_volume = validate_identifier(
        args.landing_volume
    )

    orchestrator_run_id = (
        validate_run_reference(
            args.orchestrator_run_id
        )
    )

    spark = (
        SparkSession
        .builder
        .getOrCreate()
    )

    paths = build_paths(
        catalog=catalog,
        landing_volume=landing_volume,
        orchestrator_run_id=(
            orchestrator_run_id
        ),
    )

    print(
        "============================================================"
    )

    print(
        "SERVERLESS LANDING PREFLIGHT"
    )

    print(
        "============================================================"
    )

    print(
        f"Catalog.........: {catalog}"
    )

    print(
        f"Volume..........: {landing_volume}"
    )

    print(
        f"Job run.........: {orchestrator_run_id}"
    )

    print(
        f"Data path.......: {paths['data_path']}"
    )

    print(
        f"Manifest path...: {paths['manifest_path']}"
    )

    print(
        "============================================================"
    )

    # --------------------------------------------------------
    # 1. Observation + write
    # --------------------------------------------------------

    source_row_count = (
        test_parquet_write_with_observation(
            spark=spark,
            data_path=paths["data_path"],
        )
    )

    # --------------------------------------------------------
    # 2. Read-back
    # --------------------------------------------------------

    landing_row_count = (
        test_parquet_read_back(
            spark=spark,
            data_path=paths["data_path"],
            expected_count=source_row_count,
        )
    )

    # --------------------------------------------------------
    # 3. Manifest
    # --------------------------------------------------------

    test_manifest_write(
        spark=spark,
        manifest_path=(
            paths["manifest_path"]
        ),
        data_path=(
            paths["data_path"]
        ),
        row_count=(
            landing_row_count
        ),
    )

    # --------------------------------------------------------
    # 4. Collision semantics
    # --------------------------------------------------------

    test_error_mode_collision(
        spark=spark,
        data_path=paths["data_path"],
    )

    # --------------------------------------------------------
    # 5. Final integrity
    # --------------------------------------------------------

    validate_final_state(
        spark=spark,
        data_path=paths["data_path"],
    )

    print(
        "============================================================"
    )

    print(
        "PREFLIGHT CONCLUÍDO COM SUCESSO"
    )

    print(
        "============================================================"
    )

    print(
        "Compatibilidades comprovadas:"
    )

    print(
        "  Spark Connect DataFrame.......: OK"
    )

    print(
        "  Observation...................: OK"
    )

    print(
        "  Parquet em UC Volume..........: OK"
    )

    print(
        "  Leitura física................: OK"
    )

    print(
        "  Manifest JSON.................: OK"
    )

    print(
        "  mode('error').................: OK"
    )

    print(
        "  proteção contra overwrite.....: OK"
    )


if __name__ == "__main__":
    main()
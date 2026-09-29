from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DecimalType,
    TimestampType,
)
from delta.tables import DeltaTable
import logging


# ============================================================
# 1. CONFIGURAÇÕES
# ============================================================

SOURCE_PATH = "/mnt/raw/producao_moinhos/"
TARGET_TABLE = "niometal.silver.producao_moinhos"

# Campo utilizado para identificar unicamente um registro.
# Deve ser substituído pela chave real do negócio.
BUSINESS_KEY = "id_producao"


# ============================================================
# 2. LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

logger = logging.getLogger("producao_moinhos_pipeline")


# ============================================================
# 3. SCHEMA EXPLÍCITO
# ============================================================

schema_producao = StructType([
    StructField(
        "id_producao",
        StringType(),
        False
    ),
    StructField(
        "data_producao",
        TimestampType(),
        True
    ),
    StructField(
        "toneladas_produzidas",
        DecimalType(18, 3),
        True
    ),
    StructField(
        "moinho_id",
        StringType(),
        True
    ),
    StructField(
        "linha_producao",
        StringType(),
        True
    )
])


# ============================================================
# 4. LEITURA ESCALÁVEL
# ============================================================

def ler_producao():

    logger.info(
        "Iniciando leitura dos arquivos de produção: %s",
        SOURCE_PATH
    )

    try:

        df = (
            spark.read
            .schema(schema_producao)
            .option("header", True)
            .option("mode", "FAILFAST")
            .csv(SOURCE_PATH)
        )

        logger.info(
            "Leitura dos arquivos concluída."
        )

        return df

    except Exception:
        logger.exception(
            "Erro durante a leitura dos arquivos de produção."
        )

        raise


# ============================================================
# 5. DATA QUALITY / TRANSFORMAÇÃO
# ============================================================

def tratar_producao(df):

    logger.info("Iniciando validação dos dados.")

    try:

        # Remove registros sem toneladas produzidas
        df_validos = (
            df
            .filter(
                F.col("toneladas_produzidas").isNotNull()
            )
            .filter(
                F.col("toneladas_produzidas") >= 0
            )
        )

        # Adiciona metadados de ingestão
        df_validos = (
            df_validos
            .withColumn(
                "ingestion_timestamp",
                F.current_timestamp()
            )
        )

        return df_validos

    except Exception:
        logger.exception(
            "Erro durante o tratamento dos dados."
        )

        raise


# ============================================================
# 6. CRIAÇÃO DA TABELA DELTA
# ============================================================

def criar_tabela_se_necessario():

    if not spark.catalog.tableExists(TARGET_TABLE):

        logger.info(
            "Tabela %s não existe. Criando tabela Delta.",
            TARGET_TABLE
        )

        (
            spark.createDataFrame(
                [],
                schema_producao
            )
            .withColumn(
                "ingestion_timestamp",
                F.lit(None).cast(TimestampType())
            )
            .write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(TARGET_TABLE)
        )


# ============================================================
# 7. MERGE INCREMENTAL / IDEMPOTENTE
# ============================================================

def carregar_incremental(df):

    logger.info(
        "Iniciando carga incremental."
    )

    try:

        delta_target = DeltaTable.forName(
            spark,
            TARGET_TABLE
        )

        (
            delta_target.alias("target")
            .merge(
                df.alias("source"),
                f"target.{BUSINESS_KEY} = "
                f"source.{BUSINESS_KEY}"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

        logger.info(
            "Carga incremental concluída com sucesso."
        )

    except Exception:
        logger.exception(
            "Erro durante o MERGE da tabela Delta."
        )

        raise


# ============================================================
# 8. PIPELINE PRINCIPAL
# ============================================================

def carregar_producao():

    logger.info(
        "========== INÍCIO PIPELINE PRODUÇÃO =========="
    )

    try:

        # 1. Cria tabela destino caso não exista
        criar_tabela_se_necessario()

        # 2. Leitura
        df = ler_producao()

        # 3. Tratamento
        df_limpo = tratar_producao(df)

        # 4. Carga incremental
        carregar_incremental(df_limpo)

        logger.info(
            "========== PIPELINE FINALIZADO COM SUCESSO =========="
        )

    except Exception:

        logger.exception(
            "========== FALHA NO PIPELINE DE PRODUÇÃO =========="
        )

        raise


# ============================================================
# EXECUÇÃO
# ============================================================

carregar_producao()
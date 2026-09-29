from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType
)
from delta.tables import DeltaTable
import logging


# ============================================================
# CONFIGURAÇÕES
# ============================================================

SOURCE_PATH = "/mnt/raw/cadastro_fornecedores/"
TARGET_TABLE = "niometal.silver.cadastro_fornecedores"

BUSINESS_KEY = "id_fornecedor"

# Colunas que representam os atributos do fornecedor.
# Alterações nessas colunas gerarão uma nova versão.
ATTRIBUTE_COLUMNS = [
    "nome_fornecedor",
    "cnpj",
    "endereco",
    "cidade",
    "estado",
    "categoria"
]


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

logger = logging.getLogger(
    "cadastro_fornecedores_scd2"
)


# ============================================================
# SCHEMA
# ============================================================

schema_fornecedores = StructType([
    StructField(
        "id_fornecedor",
        StringType(),
        False
    ),
    StructField(
        "nome_fornecedor",
        StringType(),
        True
    ),
    StructField(
        "cnpj",
        StringType(),
        True
    ),
    StructField(
        "endereco",
        StringType(),
        True
    ),
    StructField(
        "cidade",
        StringType(),
        True
    ),
    StructField(
        "estado",
        StringType(),
        True
    ),
    StructField(
        "categoria",
        StringType(),
        True
    )
])


# ============================================================
# LEITURA
# ============================================================

def ler_fornecedores():

    logger.info(
        "Iniciando leitura do cadastro de fornecedores."
    )

    try:

        df = (
            spark.read
            .schema(schema_fornecedores)
            .option("header", True)
            .option("mode", "FAILFAST")
            .csv(SOURCE_PATH)
        )

        return df

    except Exception:

        logger.exception(
            "Erro durante leitura do cadastro."
        )

        raise


# ============================================================
# PREPARAÇÃO DOS DADOS
# ============================================================

def preparar_fornecedores(df):

    logger.info(
        "Preparando dados para SCD Tipo 2."
    )

    # Remove registros sem chave de negócio
    df = (
        df
        .filter(
            F.col(BUSINESS_KEY).isNotNull()
        )
    )

    # Remove duplicidades dentro da própria carga
    df = (
        df
        .dropDuplicates([BUSINESS_KEY])
    )

    # Cria hash dos atributos.
    #
    # Esse hash será utilizado para identificar
    # se os atributos do fornecedor realmente mudaram.
    df = (
        df.withColumn(
            "attribute_hash",
            F.sha2(
                F.concat_ws(
                    "||",
                    *[
                        F.coalesce(
                            F.col(c).cast("string"),
                            F.lit("")
                        )
                        for c in ATTRIBUTE_COLUMNS
                    ]
                ),
                256
            )
        )
    )

    # Timestamp da ingestão
    df = (
        df.withColumn(
            "ingestion_timestamp",
            F.current_timestamp()
        )
    )

    return df


# ============================================================
# CRIAÇÃO DA TABELA DESTINO
# ============================================================

def criar_tabela_se_necessario():

    if not spark.catalog.tableExists(TARGET_TABLE):

        logger.info(
            "Tabela SCD2 não encontrada. Criando tabela."
        )

        (
            spark.createDataFrame(
                [],
                schema_fornecedores
            )
            .withColumn(
                "attribute_hash",
                F.lit(None).cast("string")
            )
            .withColumn(
                "valid_from",
                F.lit(None).cast("timestamp")
            )
            .withColumn(
                "valid_to",
                F.lit(None).cast("timestamp")
            )
            .withColumn(
                "is_current",
                F.lit(None).cast("boolean")
            )
            .withColumn(
                "ingestion_timestamp",
                F.lit(None).cast("timestamp")
            )
            .write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(TARGET_TABLE)
        )


# ============================================================
# SCD TYPE 2
# ============================================================

def aplicar_scd_tipo_2(df_source):

    logger.info(
        "Iniciando aplicação de SCD Tipo 2."
    )

    try:

        delta_target = DeltaTable.forName(
            spark,
            TARGET_TABLE
        )

        target = delta_target.toDF()

        # ----------------------------------------------------
        # 1. Identificar registros novos ou alterados
        # ----------------------------------------------------

        current_target = (
            target
            .filter(
                F.col("is_current") == True
            )
        )

        comparison = (
            df_source.alias("source")
            .join(
                current_target.alias("target"),
                F.col(
                    f"source.{BUSINESS_KEY}"
                )
                ==
                F.col(
                    f"target.{BUSINESS_KEY}"
                ),
                "left"
            )
        )

        changed_or_new = (
            comparison
            .filter(
                F.col(
                    f"target.{BUSINESS_KEY}"
                ).isNull()
                |
                (
                    F.col(
                        "source.attribute_hash"
                    )
                    !=
                    F.col(
                        "target.attribute_hash"
                    )
                )
            )
            .select(
                "source.*"
            )
        )

        # ----------------------------------------------------
        # 2. Fechar versões antigas
        # ----------------------------------------------------

        changed_existing = (
            changed_or_new.alias("source")
            .join(
                current_target.alias("target"),
                F.col(
                    f"source.{BUSINESS_KEY}"
                )
                ==
                F.col(
                    f"target.{BUSINESS_KEY}"
                ),
                "inner"
            )
            .select(
                F.col(
                    f"source.{BUSINESS_KEY}"
                ).alias(BUSINESS_KEY)
            )
        )

        # Timestamp utilizado para fechar a versão
        current_timestamp = F.current_timestamp()

        (
            delta_target.alias("target")
            .merge(
                changed_existing.alias("source"),
                f"""
                target.{BUSINESS_KEY} = source.{BUSINESS_KEY}
                AND target.is_current = true
                """
            )
            .whenMatchedUpdate(
                set={
                    "valid_to": current_timestamp,
                    "is_current": F.lit(False)
                }
            )
            .execute()
        )

        # ----------------------------------------------------
        # 3. Inserir novas versões
        # ----------------------------------------------------

        records_to_insert = (
            changed_or_new
            .withColumn(
                "valid_from",
                current_timestamp
            )
            .withColumn(
                "valid_to",
                F.lit(None).cast("timestamp")
            )
            .withColumn(
                "is_current",
                F.lit(True)
            )
        )

        (
            records_to_insert
            .write
            .format("delta")
            .mode("append")
            .saveAsTable(TARGET_TABLE)
        )

        logger.info(
            "SCD Tipo 2 aplicado com sucesso."
        )

    except Exception:

        logger.exception(
            "Erro durante aplicação do SCD Tipo 2."
        )

        raise


# ============================================================
# PIPELINE PRINCIPAL
# ============================================================

def carregar_fornecedores():

    logger.info(
        "========== INÍCIO SCD FORNECEDORES =========="
    )

    try:

        # 1. Criar tabela caso não exista
        criar_tabela_se_necessario()

        # 2. Ler origem
        df = ler_fornecedores()

        # 3. Preparar dados
        df_preparado = preparar_fornecedores(df)

        # 4. Aplicar SCD Tipo 2
        aplicar_scd_tipo_2(df_preparado)

        logger.info(
            "========== SCD FINALIZADO COM SUCESSO =========="
        )

    except Exception:

        logger.exception(
            "========== FALHA NO PIPELINE SCD =========="
        )

        raise


# ============================================================
# EXECUÇÃO
# ============================================================

carregar_fornecedores()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Schema explícito para parsing do JSON semiestruturado e aninhado
SCHEMA_SENSORES = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("leitura", StructType([
        StructField("temperatura", DoubleType(), True),
        StructField("vibracao", DoubleType(), True),
        StructField("consumo_eletrico", DoubleType(), True)
    ]))
])

def deduplicar_sensores_iot(spark: SparkSession, path_raw: str, path_silver: str, checkpoint_path: str):
    """
    Lê o stream de dados dos sensores, aplica Watermarking para dados atrasados
    e remove duplicados mantendo o estado sob controle.
    """
    # 1. Leitura do Stream via Auto Loader (CloudFiles)
    df_stream = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(SCHEMA_SENSORES)
        .load(path_raw)
    )

    # 2. Aplicação de Watermark + Deduplicação por Chave Composta
    df_deduplicado = (
        df_stream
        # Define 2 horas como janela máxima aceitável de atraso de dados (Late-Arriving)
        .withWatermark("timestamp", "2 hours")
        # Remove registos duplicados dentro da janela do Watermark
        .dropDuplicates(["sensor_id", "timestamp"])
    )

    # 3. Aplanamento (Flattening) e Escrita Atômica/Idempotente na Camada Silver
    query = (
        df_deduplicado
        .select(
            col("sensor_id"),
            col("timestamp"),
            col("leitura.temperatura").alias("temperatura"),
            col("leitura.vibracao").alias("vibracao"),
            col("leitura.consumo_eletrico").alias("consumo_eletrico")
        )
        .writeStream
        .format("delta")
        .outputMode("append")
        # O Checkpoint garante Exactly-Once semantics no reprocessamento/reinicialização
        .option("checkpointLocation", checkpoint_path)
        .start(path_silver)
    )
    
    return query